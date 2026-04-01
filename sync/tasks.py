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
from .consumers import send_sync_progress


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
    send_progress: bool = False,
    facility_index: int = 0,
    total_facilities: int = 0,
) -> dict:
    """
    Synchronize stock movements for a single facility.

    Args:
        facility_id: OpenLMIS facility UUID
        period: DHIS2 period (YYYYMM). Defaults to previous month.
        send_progress: If True, send real-time updates via channels.
        facility_index: Current facility index (for progress display).
        total_facilities: Total number of facilities being synced.

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
        if send_progress:
            send_sync_progress({
                'type': 'facility_error',
                'facility_id': facility_id,
                'facility_name': facility_id[:12],
                'message': 'Structure non trouvée ou inactive',
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')
        return {'status': 'error', 'message': f'Facility {facility_id} not found'}
    
    facility_name = facility.display_name

    # Send progress: starting
    if send_progress:
        send_sync_progress({
            'type': 'facility_starting',
            'facility_id': str(facility.openlmis_facility_id),
            'facility_name': facility_name,
            'dhis2_org_unit': facility.dhis2_org_unit_id,
            'period': period_str,
            'facility_index': facility_index,
            'total_facilities': total_facilities,
        }, msg_type='sync_facility_update')

    # Create sync log
    sync_log = SyncLog.objects.create(
        facility=facility,
        period=period_str,
        status=SyncStatus.RUNNING,
        celery_task_id=self.request.id or ''
    )
    
    result = {
        'facility_id': facility_id,
        'facility_name': facility_name,
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

        # Send progress: extracting
        if send_progress:
            send_sync_progress({
                'type': 'facility_extracting',
                'facility_id': str(facility.openlmis_facility_id),
                'facility_name': facility_name,
                'step': 'Extraction des données',
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')

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

        # Build list of attributes for progress display
        attributes_sent = []
        for dv in aggregated_data:
            attributes_sent.append({
                'data_element': dv.get('dhis2_data_element_uid', ''),
                'indicator': dv.get('indicator', ''),
                'value': dv.get('value', 0),
                'category_combo': dv.get('dhis2_category_option_combo_uid', ''),
            })

        # Send progress: extracted
        if send_progress:
            send_sync_progress({
                'type': 'facility_extracted',
                'facility_id': str(facility.openlmis_facility_id),
                'facility_name': facility_name,
                'records_extracted': len(aggregated_data),
                'attributes': attributes_sent[:20],  # Limit for display
                'total_attributes': len(attributes_sent),
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')
        
        if not aggregated_data:
            logger.warning(f"[{facility_id}] No data to push for period {period_str}")
            sync_log.status = SyncStatus.SUCCESS
            sync_log.completed_at = timezone.now()
            sync_log.details = {'message': 'No data found in database view'}
            sync_log.save()
            result['status'] = 'success'
            result['message'] = 'No data to sync'

            if send_progress:
                send_sync_progress({
                    'type': 'facility_completed',
                    'facility_id': str(facility.openlmis_facility_id),
                    'facility_name': facility_name,
                    'status': 'success',
                    'message': 'Aucune donnée à synchroniser',
                    'records_extracted': 0,
                    'records_loaded': 0,
                    'records_failed': 0,
                    'facility_index': facility_index,
                    'total_facilities': total_facilities,
                }, msg_type='sync_facility_update')
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

        # Send progress: pushing
        if send_progress:
            send_sync_progress({
                'type': 'facility_pushing',
                'facility_id': str(facility.openlmis_facility_id),
                'facility_name': facility_name,
                'step': 'Envoi vers DHIS2',
                'records_to_push': len(aggregated_data),
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')
        
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

        # Send progress: completed
        if send_progress:
            send_sync_progress({
                'type': 'facility_completed',
                'facility_id': str(facility.openlmis_facility_id),
                'facility_name': facility_name,
                'status': result['status'],
                'records_extracted': result['records_extracted'],
                'records_loaded': records_loaded,
                'records_failed': records_failed,
                'dhis2_response_summary': {
                    'imported': response.get('imported', 0),
                    'updated': response.get('updated', 0),
                    'ignored': response.get('ignored', 0),
                },
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')
        
        return result
        
    except Exception as e:
        logger.error(f"[{facility_id}] Sync failed with error: {e}")
        
        sync_log.status = SyncStatus.FAILED
        sync_log.error_message = str(e)
        sync_log.completed_at = timezone.now()
        sync_log.save()
        
        result['status'] = 'error'
        result['message'] = str(e)

        # Send progress: failed
        if send_progress:
            send_sync_progress({
                'type': 'facility_failed',
                'facility_id': facility_id,
                'facility_name': facility_name if 'facility_name' in dir() else facility_id[:12],
                'error': str(e),
                'facility_index': facility_index,
                'total_facilities': total_facilities,
            }, msg_type='sync_facility_update')
        
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


@shared_task(bind=True)
def manual_sync_task(self, period: str = None) -> dict:
    """
    Manual sync task triggered from the admin UI.
    
    Unlike sync_all_facilities_task, this task:
    - Sends real-time progress updates via Django Channels
    - Runs facility syncs sequentially for clear progress tracking
    - Reports detailed attribute information
    
    Args:
        period: DHIS2 period (YYYYMM)
        
    Returns:
        Dict with sync summary
    """
    period_str, start_date, end_date = get_period_dates(period)
    month_name = start_date.strftime('%B %Y')
    
    logger.info(f"[Manual Sync] Starting for period: {month_name}")
    
    # Get all active facilities
    facilities = list(FacilityMapping.objects.filter(is_active=True))
    total = len(facilities)
    
    if total == 0:
        send_sync_progress({
            'type': 'sync_finished',
            'status': 'warning',
            'message': 'Aucune structure active trouvée',
            'period': period_str,
        }, msg_type='sync_finished')
        return {'status': 'warning', 'message': 'No active facilities'}
    
    # Notify: sync started
    send_sync_progress({
        'type': 'sync_started',
        'period': period_str,
        'period_label': month_name,
        'total_facilities': total,
        'facilities': [
            {
                'id': str(f.openlmis_facility_id),
                'name': f.display_name,
                'dhis2_org_unit': f.dhis2_org_unit_id,
            }
            for f in facilities
        ],
    }, msg_type='sync_started')
    
    # Process facilities sequentially for clear progress
    results = {
        'success': 0,
        'partial': 0,
        'failed': 0,
        'no_data': 0,
        'details': [],
    }
    
    for idx, facility in enumerate(facilities):
        fid = str(facility.openlmis_facility_id)
        try:
            # Call sync directly (not .delay()) so we get sequential execution
            result = sync_facility_task.apply(
                kwargs={
                    'facility_id': fid,
                    'period': period_str,
                    'send_progress': True,
                    'facility_index': idx + 1,
                    'total_facilities': total,
                },
            ).get(timeout=600)  # 10 min timeout per facility
            
            status = result.get('status', 'error')
            if status == 'success':
                if result.get('records_extracted', 0) == 0:
                    results['no_data'] += 1
                else:
                    results['success'] += 1
            elif status == 'partial':
                results['partial'] += 1
            else:
                results['failed'] += 1
            
            results['details'].append(result)
            
        except Exception as e:
            logger.error(f"[Manual Sync] Failed for {fid}: {e}")
            results['failed'] += 1
            results['details'].append({
                'facility_id': fid,
                'facility_name': facility.display_name,
                'status': 'error',
                'message': str(e),
            })
    
    # Notify: sync finished
    summary = {
        'type': 'sync_finished',
        'status': 'completed',
        'period': period_str,
        'period_label': month_name,
        'total_facilities': total,
        'success_count': results['success'],
        'partial_count': results['partial'],
        'failed_count': results['failed'],
        'no_data_count': results['no_data'],
    }
    send_sync_progress(summary, msg_type='sync_finished')
    
    logger.info(f"[Manual Sync] Completed. Success: {results['success']}, "
                f"Partial: {results['partial']}, Failed: {results['failed']}, "
                f"No data: {results['no_data']}")
    
    return summary


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
