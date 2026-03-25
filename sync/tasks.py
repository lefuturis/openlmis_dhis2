"""
Celery Tasks for OpenLMIS-DHIS2 Synchronization.

This module contains the main sync tasks orchestrated by Celery Beat.
"""
from datetime import datetime, timedelta
from calendar import monthrange
from typing import Optional


from celery import shared_task, chain, group
from celery.exceptions import MaxRetriesExceededError
from django.conf import settings
from django.utils import timezone
from loguru import logger

from .models import (
    FacilityMapping, DHIS2Server, SyncLog, SyncStatus, AggregatedData
)
from .services import DatabaseExtractor, DHIS2Client


def get_period_dates(period: str = None) -> tuple:
    """
    Get start and end dates for a period.
    
    Args:
        period: DHIS2 period format YYYYMM. If None, uses previous month.
        
    Returns:
        Tuple of (period_str, start_date, end_date)
    """
    if period:
        year = int(period[:4])
        month = int(period[4:6])
    else:
        # Default to previous month
        today = datetime.now()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1
        period = f"{year}{month:02d}"
    
    start_date = datetime(year, month, 1)
    _, last_day = monthrange(year, month)
    end_date = datetime(year, month, last_day, 23, 59, 59)
    
    return period, start_date, end_date


@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={'max_retries': 3},
    acks_late=True,
)
def sync_facility_task(
    self,
    facility_id: str,
    period: str = None,
) -> dict:
    """
    Synchronize stock movements for a single facility.

    Args:
        facility_id: OpenLMIS facility UUID
        period: DHIS2 period (YYYYMM). Defaults to previous month.

    Returns:
        Dict with sync results
    """
    period_str, start_date, end_date = get_period_dates(period)
    month_name = start_date.strftime('%b %Y')

    logger.info(f"Start Sync for facility_id={facility_id} (Month: {month_name})")

    # Get facility mapping
    try:
        facility = FacilityMapping.objects.get(
            openlmis_facility_id=facility_id,
            is_active=True
        )
    except FacilityMapping.DoesNotExist:
        logger.error(f"Facility UUID {facility_id} not found or inactive")
        return {'status': 'error', 'message': f'Facility {facility_id} not found'}
    
    # Create sync log
    sync_log = SyncLog.objects.create(
        facility=facility,
        period=period_str,
        status=SyncStatus.RUNNING,
        celery_task_id=self.request.id or ''
    )
    
    result = {
        'facility_id': facility_id,
        'period': period_str,
        'status': 'pending',
        'records_extracted': 0,
        'records_transformed': 0,
        'records_loaded': 0,
        'records_failed': 0,
    }

    try:
        # =====================================================================
        # STEP A & B: Extraction and Transformation from DB View
        # =====================================================================
        logger.info(f"[{facility_id}] Step A & B: Extracting and mapping data from database view")

        start_date_str = start_date.strftime('%Y-%m-%d')
        extractor = DatabaseExtractor()

        aggregated_data = extractor.extract_monthly_data(
            facility_id=facility_id,
            period=period_str,
            start_date_str=start_date_str
        )
        
        result['records_extracted'] = len(aggregated_data)
        result['records_transformed'] = len(aggregated_data)
        
        sync_log.records_extracted = len(aggregated_data)
        sync_log.records_transformed = len(aggregated_data)
        sync_log.save(update_fields=['records_extracted', 'records_transformed'])
        
        if not aggregated_data:
            logger.warning(f"[{facility_id}] No data to push for period {period_str}")
            sync_log.status = SyncStatus.SUCCESS
            sync_log.completed_at = timezone.now()
            sync_log.details = {'message': 'No data found in database view'}
            sync_log.save()
            result['status'] = 'success'
            result['message'] = 'No data to sync'
            return result
        
        # Store aggregated data for audit
        for data in aggregated_data:
            AggregatedData.objects.update_or_create(
                facility=facility,
                period=period_str,
                openlmis_product_id=data.get('product_id', ''),
                indicator=data.get('indicator', ''),
                defaults={
                    'value': data.get('value', 0),
                    'dhis2_data_element_uid': data.get('dhis2_data_element_uid', ''),
                    'dhis2_category_option_combo_uid': data.get('dhis2_category_option_combo_uid', ''),
                    'dhis2_org_unit_id': facility.dhis2_org_unit_id,
                    'is_synced': False,
                }
            )
        
        # =====================================================================
        # STEP C: Loading to DHIS2
        # =====================================================================
        logger.info(f"[{facility_id}] Step C: Pushing data to DHIS2")
        
        with DHIS2Client(server=facility.server) as dhis2:
            response = dhis2.submit_data_values(
                data_values=aggregated_data,
                org_unit_id=facility.dhis2_org_unit_id,
                period=period_str
            )
        
        # Process response
        records_loaded = response.get('imported', 0) + response.get('updated', 0)
        records_failed = response.get('ignored', 0)
        
        result['records_loaded'] = records_loaded
        result['records_failed'] = records_failed
        result['dhis2_response'] = response
        
        sync_log.records_loaded = records_loaded
        sync_log.records_failed = records_failed
        sync_log.details = response
        
        # Mark aggregated data as synced
        if response.get('success'):
            AggregatedData.objects.filter(
                facility=facility,
                period=period_str,
                is_synced=False
            ).update(is_synced=True, synced_at=timezone.now())

            sync_log.status = SyncStatus.SUCCESS
            result['status'] = 'success'
            logger.info(f"[{facility_id}] Sync completed. "
                        f"Loaded: {records_loaded}, Failed: {records_failed}")
        else:
            if records_loaded > 0:
                sync_log.status = SyncStatus.PARTIAL
                result['status'] = 'partial'
            else:
                sync_log.status = SyncStatus.FAILED
                result['status'] = 'failed'

            sync_log.error_message = response.get('message', 'Unknown error')
            logger.error(f"[{facility_id}] Sync completed with errors: {response}")
        
        sync_log.completed_at = timezone.now()
        sync_log.save()
        
        return result
        
    except Exception as e:
        logger.error(f"[{facility_id}] Sync failed with error: {e}")
        
        sync_log.status = SyncStatus.FAILED
        sync_log.error_message = str(e)
        sync_log.completed_at = timezone.now()
        sync_log.save()
        
        result['status'] = 'error'
        result['message'] = str(e)
        
        # Re-raise to trigger Celery retry
        raise


@shared_task(bind=True)
def sync_all_facilities_task(self, period: str = None) -> dict:
    """
    Trigger sync for all active facilities.
    
    This is the main periodic task scheduled by Celery Beat.
    It spawns individual sync_facility_task for each active facility.
    
    Args:
        period: DHIS2 period (YYYYMM). Defaults to previous month.
        
    Returns:
        Dict with task dispatch summary
    """
    period_str, start_date, end_date = get_period_dates(period)
    month_name = start_date.strftime('%b %Y')
    
    logger.info(f"Starting monthly sync for all facilities (Period: {month_name})")
    
    # Get all active facilities
    facilities = FacilityMapping.objects.filter(is_active=True)
    facility_count = facilities.count()
    
    if facility_count == 0:
        logger.warning("No active facilities found for sync")
        return {
            'status': 'warning',
            'message': 'No active facilities to sync',
            'period': period_str
        }
    
    logger.info(f"Dispatching sync tasks for {facility_count} facilities")
    
    # Dispatch individual tasks
    task_results = []
    for facility in facilities:
        fid = str(facility.openlmis_facility_id)
        task = sync_facility_task.delay(
            facility_id=fid,
            period=period_str
        )
        task_results.append({
            'facility_id': fid,
            'task_id': task.id
        })
        logger.debug(f"Dispatched sync task for facility_id={fid}: {task.id}")
    
    return {
        'status': 'dispatched',
        'period': period_str,
        'facilities_count': facility_count,
        'tasks': task_results
    }


@shared_task
def retry_failed_syncs(period: str = None, max_retries: int = 3) -> dict:
    """
    Retry failed sync operations for a period.
    
    Args:
        period: DHIS2 period to retry
        max_retries: Maximum number of retry attempts
        
    Returns:
        Dict with retry results
    """
    period_str, _, _ = get_period_dates(period)
    
    # Find failed syncs
    failed_syncs = SyncLog.objects.filter(
        period=period_str,
        status=SyncStatus.FAILED
    )
    
    logger.info(f"Found {failed_syncs.count()} failed syncs to retry for period {period_str}")
    
    retry_results = []
    for sync_log in failed_syncs:
        if sync_log.facility:
            fid = str(sync_log.facility.openlmis_facility_id)
            task = sync_facility_task.delay(
                facility_id=fid,
                period=period_str
            )
            retry_results.append({
                'facility_id': fid,
                'task_id': task.id
            })
    
    return {
        'status': 'dispatched',
        'period': period_str,
        'retried_count': len(retry_results),
        'tasks': retry_results
    }


@shared_task
def cleanup_old_logs(days: int = 90) -> dict:
    """
    Clean up old sync logs.
    
    Args:
        days: Delete logs older than this many days
        
    Returns:
        Dict with cleanup results
    """
    cutoff_date = timezone.now() - timedelta(days=days)
    
    deleted_count, _ = SyncLog.objects.filter(
        started_at__lt=cutoff_date
    ).delete()
    
    logger.info(f"Cleaned up {deleted_count} sync logs older than {days} days")
    
    return {
        'status': 'success',
        'deleted_count': deleted_count,
        'cutoff_date': cutoff_date.isoformat()
    }


@shared_task(bind=True)
def trigger_sync_from_cli(self, period: str = None, auto: bool = False, force: bool = False) -> dict:
    """
    Task déclenchable par la CLI ou CeleryBeat.
    
    Cette task sert de point d'entrée unifié pour:
    - La commande CLI sync_manager.py
    - Les tâches planifiées CeleryBeat
    - Les déclenchements manuels via Flower/API
    
    Args:
        period: Période spécifique au format YYYYMM (ex: 202510)
        auto: Si True, calcule automatiquement le mois précédent
        force: Re-exécuter même si déjà synchronisé pour cette période
        
    Returns:
        Dict avec le résumé de l'exécution
    """
    # Déterminer la période cible
    if period:
        target_period = period
    elif auto:
        # Calculer le mois précédent
        today = datetime.now()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1
        target_period = f"{year}{month:02d}"
        logger.info(f"Mode Auto: Période calculée = {target_period}")
    else:
        # Par défaut, utiliser le mois précédent
        today = datetime.now()
        if today.month == 1:
            year = today.year - 1
            month = 12
        else:
            year = today.year
            month = today.month - 1
        target_period = f"{year}{month:02d}"
    
    logger.info(f"Déclenchement synchronisation pour période: {target_period} (auto={auto}, force={force})")
    
    # Déléguer à sync_all_facilities_task
    result = sync_all_facilities_task.delay(period=target_period)
    
    return {
        'status': 'dispatched',
        'period': target_period,
        'auto_mode': auto,
        'force': force,
        'parent_task_id': self.request.id,
        'child_task_id': result.id,
    }
