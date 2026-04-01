"""
Django Admin configuration for Sync models.
Includes custom admin view for manual synchronization with real-time progress.
"""
from django.contrib import admin
from django.urls import path
from django.template.response import TemplateResponse
from django.contrib.admin.views.decorators import staff_member_required

from .models import (
    DHIS2Server, FacilityMapping, DataElementMapping,
    DataSet, SyncSchedule, SyncLog, AggregatedData
)


# =============================================================================
# Custom Admin Site with Manual Sync View
# =============================================================================
class SyncAdminSite(admin.AdminSite):
    """Extended admin site with manual sync page."""
    site_header = "OpenLMIS → DHIS2 Administration"
    site_title = "OpenLMIS-DHIS2"
    index_title = "Tableau de bord"

    def get_urls(self):
        custom_urls = [
            path(
                'sync/manual-sync/',
                self.admin_view(self.manual_sync_view),
                name='manual-sync',
            ),
        ]
        return custom_urls + super().get_urls()

    def manual_sync_view(self, request):
        """Render the manual sync page with year/month selector."""
        context = {
            **self.each_context(request),
            'title': 'Synchronisation Manuelle',
        }
        return TemplateResponse(
            request,
            'admin/sync/manual_sync.html',
            context,
        )


# Replace the default admin site
admin_site = SyncAdminSite(name='admin')

# Re-register django.contrib.auth models
from django.contrib.auth.admin import UserAdmin, GroupAdmin
from django.contrib.auth.models import User, Group
admin_site.register(User, UserAdmin)
admin_site.register(Group, GroupAdmin)

# Re-register django_celery_beat models
try:
    from django_celery_beat.admin import (
        ClockedScheduleAdmin, CrontabScheduleAdmin,
        IntervalScheduleAdmin, PeriodicTaskAdmin,
        SolarScheduleAdmin,
    )
    from django_celery_beat.models import (
        ClockedSchedule, CrontabSchedule,
        IntervalSchedule, PeriodicTask,
        SolarSchedule,
    )
    admin_site.register(ClockedSchedule, ClockedScheduleAdmin)
    admin_site.register(CrontabSchedule, CrontabScheduleAdmin)
    admin_site.register(IntervalSchedule, IntervalScheduleAdmin)
    admin_site.register(PeriodicTask, PeriodicTaskAdmin)
    admin_site.register(SolarSchedule, SolarScheduleAdmin)
except Exception:
    pass

# Re-register django_celery_results models
try:
    from django_celery_results.admin import TaskResultAdmin, GroupResultAdmin
    from django_celery_results.models import TaskResult, GroupResult
    admin_site.register(TaskResult, TaskResultAdmin)
    admin_site.register(GroupResult, GroupResultAdmin)
except Exception:
    pass


# =============================================================================
# Model Admin registrations (on custom admin site)
# =============================================================================
@admin.register(DHIS2Server, site=admin_site)
class DHIS2ServerAdmin(admin.ModelAdmin):
    list_display = ['name', 'url', 'is_active', 'created_at']
    list_filter = ['is_active']
    search_fields = ['name', 'url']


@admin.register(FacilityMapping, site=admin_site)
class FacilityMappingAdmin(admin.ModelAdmin):
    list_display = ['name', 'openlmis_facility_id', 'dhis2_org_unit_id', 'is_active', 'server']
    list_filter = ['is_active', 'server']
    search_fields = ['name', 'openlmis_facility_id', 'dhis2_org_unit_id']


@admin.register(DataElementMapping, site=admin_site)
class DataElementMappingAdmin(admin.ModelAdmin):
    list_display = ['openlmis_product_id', 'indicator', 'dhis2_data_element_uid', 'is_active']
    list_filter = ['indicator', 'is_active', 'dataset']
    search_fields = ['openlmis_product_id', 'dhis2_data_element_uid']


@admin.register(DataSet, site=admin_site)
class DataSetAdmin(admin.ModelAdmin):
    list_display = ['name', 'dhis2_dataset_uid', 'period_type', 'is_active', 'server']
    list_filter = ['period_type', 'is_active', 'server']
    search_fields = ['name', 'dhis2_dataset_uid']


@admin.register(SyncSchedule, site=admin_site)
class SyncScheduleAdmin(admin.ModelAdmin):
    list_display = ['name', 'cron_expression', 'is_active', 'last_run', 'next_run']
    list_filter = ['is_active']
    search_fields = ['name']
    filter_horizontal = ['facilities', 'datasets']


@admin.register(SyncLog, site=admin_site)
class SyncLogAdmin(admin.ModelAdmin):
    list_display = ['facility', 'period', 'status', 'started_at', 'completed_at',
                    'records_extracted', 'records_loaded', 'records_failed']
    list_filter = ['status', 'started_at']
    search_fields = ['facility__openlmis_facility_id', 'period', 'celery_task_id']
    readonly_fields = ['started_at', 'completed_at', 'details']
    date_hierarchy = 'started_at'


@admin.register(AggregatedData, site=admin_site)
class AggregatedDataAdmin(admin.ModelAdmin):
    list_display = ['facility', 'period', 'openlmis_product_id', 'indicator', 'value', 'is_synced']
    list_filter = ['is_synced', 'indicator', 'period']
    search_fields = ['openlmis_product_id', 'facility__openlmis_facility_id']
