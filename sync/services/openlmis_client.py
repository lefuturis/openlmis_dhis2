"""
OpenLMIS API Client.

Handles OAuth2 authentication and stock movements extraction with pagination.
"""
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from django.conf import settings
from loguru import logger


class OpenLMISClient:
    """
    Client for interacting with the OpenLMIS API.
    
    Handles:
    - OAuth2 authentication (client credentials + password grant)
    - Stock card retrieval with pagination
    - Stock events/movements extraction
    """
    
    def __init__(self, base_url: str = None):
        """
        Initialize OpenLMIS client.
        
        Args:
            base_url: OpenLMIS instance URL. Defaults to settings.
        """
        config = settings.OPENLMIS_CONFIG
        self.base_url = (base_url or config['BASE_URL']).rstrip('/')
        self.client_id = config['CLIENT_ID']
        self.client_secret = config['CLIENT_SECRET']
        self.username = config['USERNAME']
        self.password = config['PASSWORD']
        self.default_program_id = config.get('DEFAULT_PROGRAM_ID', '')
        self.token_endpoint = config['TOKEN_ENDPOINT']
        self.stock_cards_endpoint = config['STOCK_CARDS_ENDPOINT']
        self.stock_card_summaries_endpoint = config['STOCK_CARD_SUMMARIES_ENDPOINT']
        
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._session = requests.Session()
        
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
    
    def _get_access_token(self) -> str:
        """
        Obtain OAuth2 access token using password grant.
        
        Returns:
            Access token string.
            
        Raises:
            Exception: If authentication fails.
        """
        # Check if we have a valid token
        if self._access_token and self._token_expires_at:
            if datetime.now() < self._token_expires_at - timedelta(minutes=1):
                return self._access_token
        
        logger.debug(f"Requesting new OAuth2 token from {self.base_url}")
        
        url = f"{self.base_url}{self.token_endpoint}"
        
        # OpenLMIS uses password grant with basic auth
        response = self._session.post(
            url,
            auth=(self.client_id, self.client_secret),
            data={
                'grant_type': 'password',
                'username': self.username,
                'password': self.password,
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"OAuth2 authentication failed: {response.status_code} - {response.text}")
            raise Exception(f"Authentication failed: {response.status_code}")
        
        data = response.json()
        self._access_token = data['access_token']
        expires_in = data.get('expires_in', 3600)
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        logger.info("Successfully obtained OAuth2 access token")
        return self._access_token
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make an authenticated API request.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        token = self._get_access_token()
        
        headers = kwargs.pop('headers', {})
        headers['Authorization'] = f'Bearer {token}'
        headers['Content-Type'] = 'application/json'
        
        url = f"{self.base_url}{endpoint}"
        
        response = self._session.request(
            method,
            url,
            headers=headers,
            timeout=kwargs.pop('timeout', 60),
            **kwargs
        )
        
        return response
    
    def get_stock_card_summaries(
        self, 
        facility_id: str, 
        program_id: str = None,
        page: int = 0,
        size: int = None
    ) -> Dict[str, Any]:
        """
        Get stock card summaries for a facility.
        
        Args:
            facility_id: OpenLMIS facility UUID
            program_id: Optional program UUID filter
            page: Page number (0-indexed)
            size: Page size
            
        Returns:
            API response with stock card summaries
        """
        size = size or settings.SYNC_CONFIG.get('PAGE_SIZE', 100)
        
        params = {
            'facility': facility_id,  # API expects 'facility' not 'facilityId'
            'page': page,
            'size': size,
        }
        if program_id:
            params['program'] = program_id  # API expects 'program' not 'programId'
        
        logger.debug(f"Fetching stock card summaries: facility={facility_id}, program={program_id}, page={page}")
        
        response = self._make_request(
            'GET',
            self.stock_card_summaries_endpoint,
            params=params
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get stock card summaries: {response.status_code}")
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        return response.json()
    
    def get_stock_card(self, stock_card_id: str) -> Dict[str, Any]:
        """
        Get a specific stock card with line items.
        
        Args:
            stock_card_id: Stock card UUID
            
        Returns:
            Stock card details with line items
        """
        response = self._make_request(
            'GET',
            f"{self.stock_cards_endpoint}/{stock_card_id}"
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get stock card {stock_card_id}: {response.status_code}")
            raise Exception(f"API error: {response.status_code}")
        
        return response.json()
    
    def get_programs_for_facility(self, facility_id: str) -> List[Dict[str, Any]]:
        """
        Get all programs supported by a facility.
        
        Args:
            facility_id: OpenLMIS facility UUID
            
        Returns:
            List of program objects
        """
        response = self._make_request(
            'GET',
            '/api/facilities/' + facility_id + '/supportedPrograms'
        )
        
        if response.status_code != 200:
            logger.warning(f"Failed to get programs for facility {facility_id}: {response.status_code}")
            # Try alternative endpoint
            response = self._make_request(
                'GET',
                '/api/supportedPrograms',
                params={'facilityId': facility_id}
            )
            if response.status_code != 200:
                return []
        
        data = response.json()
        if isinstance(data, list):
            return data
        return data.get('content', data.get('supportedPrograms', []))
    
    def get_program_by_code(self, program_code: str) -> Optional[Dict[str, Any]]:
        """
        Get program details by code.
        
        Args:
            program_code: Program code (e.g., 'EPI')
            
        Returns:
            Program details or None
        """
        response = self._make_request(
            'GET',
            '/api/programs',
            params={'code': program_code}
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get program {program_code}: {response.status_code}")
            return None
        
        data = response.json()
        content = data.get('content', [])
        return content[0] if content else None

    def get_stock_movements(
        self,
        facility_id: str,
        start_date: datetime,
        end_date: datetime,
        program_id: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get all stock movements for a facility within a date range.
        
        This method handles pagination automatically and returns all movements.
        If no program_id is provided, it fetches movements for all programs
        supported by the facility.
        
        Args:
            facility_id: OpenLMIS facility UUID
            start_date: Period start date
            end_date: Period end date
            program_id: Optional program UUID filter
            
        Returns:
            List of all stock movements (line items)
        """
        all_movements = []
        page_size = settings.SYNC_CONFIG.get('PAGE_SIZE', 100)
        
        logger.info(f"Extracting stock movements for facility {facility_id} "
                   f"from {start_date.date()} to {end_date.date()}")
        
        # If no program_id provided, get all programs for this facility
        program_ids = []
        if program_id:
            program_ids = [program_id]
        else:
            programs = self.get_programs_for_facility(facility_id)
            if programs:
                for prog in programs:
                    prog_id = prog.get('program', {}).get('id') or prog.get('id')
                    if prog_id:
                        program_ids.append(prog_id)
                logger.info(f"Found {len(program_ids)} programs for facility")
            
            if not program_ids:
                # Use default program if configured
                if self.default_program_id:
                    logger.info(f"Using default program: {self.default_program_id}")
                    program_ids = [self.default_program_id]
                else:
                    logger.warning(f"No programs found for facility {facility_id} and no default program configured")
                    return []
        
        for prog_id in program_ids:
            page = 0
            
            while True:
                # Get stock card summaries (paginated)
                try:
                    summaries = self.get_stock_card_summaries(
                        facility_id=facility_id,
                        program_id=prog_id,
                        page=page,
                        size=page_size
                    )
                except Exception as e:
                    logger.error(f"Error fetching page {page} for program {prog_id}: {e}")
                    break
                
                content = summaries.get('content', [])
                if not content:
                    break
                
                # For each stock card, get detailed line items
                for summary in content:
                    stock_card_id = summary.get('id')
                    if not stock_card_id:
                        continue
                    
                    try:
                        stock_card = self.get_stock_card(stock_card_id)
                        line_items = stock_card.get('lineItems', [])
                        
                        # Filter by date range and add product info
                        orderable = stock_card.get('orderable', {})
                        product_code = orderable.get('productCode', '')
                        product_name = orderable.get('fullProductName', '')
                        
                        for item in line_items:
                            occurred_date = item.get('occurredDate')
                            if occurred_date:
                                item_date = datetime.fromisoformat(occurred_date.replace('Z', '+00:00'))
                                if start_date <= item_date <= end_date:
                                    item['productCode'] = product_code
                                    item['productName'] = product_name
                                    item['stockCardId'] = stock_card_id
                                    item['programId'] = prog_id
                                    all_movements.append(item)
                        
                    except Exception as e:
                        logger.warning(f"Error fetching stock card {stock_card_id}: {e}")
                        continue
                
                # Check if there are more pages
                total_pages = summaries.get('totalPages', 1)
                page += 1
                if page >= total_pages:
                    break
                
                logger.debug(f"Processed page {page}/{total_pages} for program {prog_id}")
        
        logger.info(f"Extracted {len(all_movements)} stock movements for facility {facility_id}")
        return all_movements
    
    def get_facility_by_code(self, facility_code: str) -> Optional[Dict[str, Any]]:
        """
        Get facility details by code.
        
        Args:
            facility_code: Facility code
            
        Returns:
            Facility details or None
        """
        response = self._make_request(
            'GET',
            '/api/facilities',
            params={'code': facility_code}
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to get facility {facility_code}: {response.status_code}")
            return None
        
        data = response.json()
        content = data.get('content', [])
        return content[0] if content else None
    
    def close(self):
        """Close the session."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
