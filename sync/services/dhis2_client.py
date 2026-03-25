"""
DHIS2 API Client.

Handles data value set submission to DHIS2.
"""
import requests
from typing import List, Dict, Any, Optional
from django.conf import settings
from loguru import logger

from ..models import DHIS2Server


class DHIS2Client:
    """
    Client for submitting data to DHIS2.
    
    Handles:
    - Basic authentication
    - DataValueSet submission
    - Response parsing and error handling
    """
    
    def __init__(self, server: DHIS2Server = None):
        """
        Initialize DHIS2 client.
        
        Args:
            server: Optional DHIS2Server model instance. If None, uses .env / settings.
        """
        self.server = server
        if server:
            self.base_url = server.url.rstrip('/')
            self.username = server.username
            self.password = server.password
        else:
            dhis2_config = settings.DHIS2_CONFIG
            self.base_url = dhis2_config.get('BASE_URL', '').rstrip('/')
            self.username = dhis2_config.get('USERNAME', '')
            self.password = dhis2_config.get('PASSWORD', '')
            
            if not self.base_url:
                raise ValueError("No DHIS2 URL configured in settings (.env)")
        
        self._session = requests.Session()
        self._session.auth = (self.username, self.password)
        
        # Configure retries
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=settings.SYNC_CONFIG.get('MAX_RETRIES', 3),
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        **kwargs
    ) -> requests.Response:
        """
        Make an authenticated API request.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request arguments
            
        Returns:
            Response object
        """
        url = f"{self.base_url}{endpoint}"
        
        headers = kwargs.pop('headers', {})
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/json'
        
        response = self._session.request(
            method,
            url,
            headers=headers,
            timeout=kwargs.pop('timeout', 60),
            **kwargs
        )
        
        return response
    
    def submit_data_values(
        self,
        data_values: List[Dict[str, Any]],
        org_unit_id: str,
        period: str,
        dataset_uid: str = None
    ) -> Dict[str, Any]:
        """
        Submit data values to DHIS2.
        
        Args:
            data_values: List of aggregated data values
            org_unit_id: DHIS2 organisation unit UID
            period: DHIS2 period format (YYYYMM)
            dataset_uid: Optional dataset UID for validation
            
        Returns:
            API response with import summary
        """
        if not data_values:
            logger.warning("No data values to submit")
            return {'status': 'WARNING', 'message': 'No data values to submit'}
        
        # Build the DataValueSet payload
        payload = {
            'dataValues': []
        }
        
        for dv in data_values:
            data_value = {
                'dataElement': dv['dhis2_data_element_uid'],
                'period': period,
                'orgUnit': org_unit_id,
                'value': str(dv['value']),
            }
            
            # Add category option combo if specified
            coc = dv.get('dhis2_category_option_combo_uid')
            if coc:
                data_value['categoryOptionCombo'] = coc
            
            payload['dataValues'].append(data_value)
        
        logger.info(f"Submitting {len(payload['dataValues'])} data values "
                   f"to DHIS2 for org unit {org_unit_id} (period: {period})")
        logger.debug(f"Payload: {payload}")
        
        # Submit to DHIS2
        try:
            response = self._make_request(
                'POST',
                '/api/dataValueSets',
                json=payload,
                params={'importStrategy': 'CREATE_AND_UPDATE'}
            )
            
            result = self._parse_response(response)
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error submitting to DHIS2: {e}")
            raise
    
    def _parse_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Parse DHIS2 API response.
        
        Args:
            response: HTTP response from DHIS2
            
        Returns:
            Parsed result with status and details
        """
        result = {
            'http_status': response.status_code,
            'success': False,
            'imported': 0,
            'updated': 0,
            'ignored': 0,
            'deleted': 0,
            'conflicts': [],
            'message': ''
        }
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Parse import summary - can be at root level or nested in 'response'
                import_count = data.get('importCount', {})
                if not import_count and 'response' in data:
                    import_count = data['response'].get('importCount', {})
                
                result['imported'] = import_count.get('imported', 0)
                result['updated'] = import_count.get('updated', 0)
                result['ignored'] = import_count.get('ignored', 0)
                result['deleted'] = import_count.get('deleted', 0)
                
                # Check response status - can be at root or nested
                status = data.get('status', '')
                if not status and 'response' in data:
                    status = data['response'].get('status', '')
                result['success'] = status in ('OK', 'SUCCESS')
                result['status'] = status
                
                # Parse conflicts from nested response
                conflicts = data.get('conflicts', [])
                if not conflicts and 'response' in data:
                    conflicts = data['response'].get('conflicts', [])
                result['conflicts'] = conflicts
                
                for conflict in conflicts:
                    logger.warning(f"DHIS2 Conflict: {conflict}")
                
                # Check for import summaries (detailed response)
                if 'response' in data:
                    resp = data['response']
                    if 'importSummaries' in resp:
                        for summary in resp['importSummaries']:
                            if summary.get('status') != 'SUCCESS':
                                logger.warning(f"Import issue: {summary}")
                
                total = result['imported'] + result['updated']
                logger.info(f"DHIS2 import complete: {total} values processed "
                           f"(imported: {result['imported']}, updated: {result['updated']}, "
                           f"ignored: {result['ignored']})")
                
                if conflicts:
                    logger.warning(f"DHIS2 import had {len(conflicts)} conflicts")
                
            except ValueError as e:
                logger.error(f"Failed to parse DHIS2 response: {e}")
                result['message'] = f"Failed to parse response: {response.text[:200]}"
        
        elif response.status_code == 409:
            # Conflict - usually validation errors
            try:
                data = response.json()
                result['message'] = data.get('message', 'Conflict')
                result['conflicts'] = data.get('response', {}).get('conflicts', [])
                logger.error(f"DHIS2 conflict: {result['message']}")
            except ValueError:
                result['message'] = response.text[:500]
                logger.error(f"DHIS2 conflict (raw): {result['message']}")
        
        elif response.status_code >= 500:
            result['message'] = f"Server error: {response.status_code}"
            logger.error(f"DHIS2 server error: {response.status_code} - {response.text[:500]}")
        
        else:
            result['message'] = f"Unexpected status: {response.status_code}"
            logger.error(f"Unexpected DHIS2 response: {response.status_code} - {response.text[:500]}")
        
        return result
    
    def validate_data_elements(self, data_element_uids: List[str]) -> Dict[str, bool]:
        """
        Validate that data elements exist in DHIS2.
        
        Args:
            data_element_uids: List of data element UIDs to validate
            
        Returns:
            Dict mapping UID to exists (True/False)
        """
        results = {}
        
        for uid in data_element_uids:
            try:
                response = self._make_request(
                    'GET',
                    f'/api/dataElements/{uid}',
                    params={'fields': 'id,name'}
                )
                results[uid] = response.status_code == 200
            except Exception:
                results[uid] = False
        
        return results
    
    def get_org_unit(self, org_unit_id: str) -> Optional[Dict[str, Any]]:
        """
        Get organisation unit details.
        
        Args:
            org_unit_id: Organisation unit UID
            
        Returns:
            Org unit details or None
        """
        try:
            response = self._make_request(
                'GET',
                f'/api/organisationUnits/{org_unit_id}',
                params={'fields': 'id,name,code'}
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Failed to get org unit {org_unit_id}: {e}")
        
        return None
    
    def test_connection(self) -> bool:
        """
        Test connection to DHIS2.
        
        Returns:
            True if connection successful
        """
        try:
            response = self._make_request('GET', '/api/system/info')
            return response.status_code == 200
        except Exception as e:
            logger.error(f"DHIS2 connection test failed: {e}")
            return False
    
    def close(self):
        """Close the session."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
