"""
API views for sync operations.
"""
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from .models import SyncLog, FacilityMapping
import json


@require_http_methods(["GET"])
def sync_status(request):
    """Get current sync status."""
    latest_logs = SyncLog.objects.order_by('-started_at')[:10]
    return JsonResponse({
        'status': 'ok',
        'recent_syncs': [
            {
                'facility': log.facility.openlmis_facility_code if log.facility else None,
                'period': log.period,
                'status': log.status,
                'started_at': log.started_at.isoformat() if log.started_at else None,
                'completed_at': log.completed_at.isoformat() if log.completed_at else None,
            }
            for log in latest_logs
        ]
    })


@csrf_exempt
@require_http_methods(["POST"])
def trigger_sync(request):
    """Manually trigger a sync operation."""
    try:
        data = json.loads(request.body) if request.body else {}
        period = data.get('period')  # Format: YYYYMM (e.g., 202511)
        facility_codes = data.get('facilities', [])  # Optional: specific facilities
        
        from .tasks import sync_all_facilities_task, sync_facility_task
        
        if facility_codes:
            # Sync specific facilities
            task_ids = []
            for code in facility_codes:
                task = sync_facility_task.delay(code, period)
                task_ids.append(task.id)
            return JsonResponse({
                'status': 'triggered',
                'task_ids': task_ids,
                'facilities': facility_codes
            })
        else:
            # Sync all facilities
            task = sync_all_facilities_task.delay(period)
            return JsonResponse({
                'status': 'triggered',
                'task_id': task.id,
                'message': 'Sync triggered for all active facilities'
            })
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@require_http_methods(["GET"])
def sync_logs(request):
    """Get sync logs with optional filtering."""
    facility = request.GET.get('facility')
    period = request.GET.get('period')
    status = request.GET.get('status')
    limit = int(request.GET.get('limit', 50))
    
    logs = SyncLog.objects.all()
    
    if facility:
        logs = logs.filter(facility__openlmis_facility_code=facility)
    if period:
        logs = logs.filter(period=period)
    if status:
        logs = logs.filter(status=status)
    
    logs = logs.order_by('-started_at')[:limit]
    
    return JsonResponse({
        'count': logs.count(),
        'logs': [
            {
                'id': log.id,
                'facility': log.facility.openlmis_facility_code if log.facility else None,
                'period': log.period,
                'status': log.status,
                'started_at': log.started_at.isoformat() if log.started_at else None,
                'completed_at': log.completed_at.isoformat() if log.completed_at else None,
                'records_extracted': log.records_extracted,
                'records_transformed': log.records_transformed,
                'records_loaded': log.records_loaded,
                'records_failed': log.records_failed,
                'error_message': log.error_message,
            }
            for log in logs
        ]
    })
