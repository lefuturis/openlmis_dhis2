"""
Django Admin configuration for Sync models.
"""
from django.contrib import admin
from .models import (
    DHIS2Server, FacilityMapping, DataElementMapping,
    DataSet, SyncSchedule, SyncLog, AggregatedData
)


@admin.register(DHIS2Server)
class DHIS2ServerAdmin(admin.ModelAdmin):
    list_display = ['name', 'url', 'is_active', 'created_at']
    list_filter = ['is_active']
    search_fields = ['name', 'url']


@admin.register(FacilityMapping)
class FacilityMappingAdmin(admin.ModelAdmin):
    list_display = ['openlmis_facility_id', 'dhis2_org_unit_id', 'is_active', 'server']
    list_filter = ['is_active', 'server']
    search_fields = ['openlmis_facility_id', 'dhis2_org_unit_id']


@admin.register(DataElementMapping)
class DataElementMappingAdmin(admin.ModelAdmin):
    list_display = ['openlmis_product_id', 'indicator', 'dhis2_data_element_uid', 'is_active']
    list_filter = ['indicator', 'is_active', 'dataset']
    search_fields = ['openlmis_product_id', 'dhis2_data_element_uid']


@admin.register(DataSet)
class DataSetAdmin(admin.ModelAdmin):
    list_display = ['name', 'dhis2_dataset_uid', 'period_type', 'is_active', 'server']
    list_filter = ['period_type', 'is_active', 'server']
    search_fields = ['name', 'dhis2_dataset_uid']


@admin.register(SyncSchedule)
class SyncScheduleAdmin(admin.ModelAdmin):
    list_display = ['name', 'cron_expression', 'is_active', 'last_run', 'next_run']
    list_filter = ['is_active']
    search_fields = ['name']
    filter_horizontal = ['facilities', 'datasets']


@admin.register(SyncLog)
class SyncLogAdmin(admin.ModelAdmin):
    list_display = ['facility', 'period', 'status', 'started_at', 'completed_at',
                    'records_extracted', 'records_loaded', 'records_failed']
    list_filter = ['status', 'started_at']
    search_fields = ['facility__openlmis_facility_id', 'period', 'celery_task_id']
    readonly_fields = ['started_at', 'completed_at', 'details']
    date_hierarchy = 'started_at'


@admin.register(AggregatedData)
class AggregatedDataAdmin(admin.ModelAdmin):
    list_display = ['facility', 'period', 'openlmis_product_id', 'indicator', 'value', 'is_synced']
    list_filter = ['is_synced', 'indicator', 'period']
    search_fields = ['openlmis_product_id', 'facility__openlmis_facility_id']
