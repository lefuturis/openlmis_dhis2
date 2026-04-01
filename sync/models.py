"""
Django Models for OpenLMIS-DHIS2 Synchronization.

These models store configuration data and track sync history.
IDs are UUIDs matching the analytics.fact_stock_monthly view.
"""
from django.db import models
from django.utils import timezone


class DHIS2Server(models.Model):
    """
    DHIS2 server connection credentials.
    """
    name = models.CharField(max_length=100, unique=True)
    url = models.URLField()
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'dhis2_servers'
        verbose_name = 'DHIS2 Server'
        verbose_name_plural = 'DHIS2 Servers'

    def __str__(self):
        return f"{self.name} ({self.url})"


class FacilityMapping(models.Model):
    """
    Mapping between OpenLMIS facility UUID and DHIS2 organisation unit UID.
    Source: analytics.fact_stock_monthly.facility_id
    """
    name = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text="Nom lisible de la structure de santé"
    )
    openlmis_facility_id = models.UUIDField(
        unique=True,
        db_index=True,
        help_text="OpenLMIS facility UUID (from kafka_facilities.id)"
    )
    dhis2_org_unit_id = models.CharField(
        max_length=11,
        db_index=True,
        help_text="DHIS2 Organisation Unit UID"
    )
    is_active = models.BooleanField(default=True)
    server = models.ForeignKey(
        DHIS2Server,
        on_delete=models.CASCADE,
        related_name='facilities',
        null=True,
        blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'facility_mappings'
        verbose_name = 'Facility Mapping'
        verbose_name_plural = 'Facility Mappings'
        indexes = [
            models.Index(fields=['openlmis_facility_id', 'is_active']),
        ]

    def __str__(self):
        label = self.name or str(self.openlmis_facility_id)
        return f"{label} -> {self.dhis2_org_unit_id}"

    @property
    def display_name(self):
        """Return name if set, otherwise short UUID."""
        return self.name or str(self.openlmis_facility_id)[:12]


class IndicatorType(models.TextChoices):
    """
    Stock movement indicators mapped to analytics.fact_stock_monthly columns.
    """
    OPENING_BALANCE = 'OPENING_BALANCE', 'Opening Balance'    # opening_balance
    RECEIPTS        = 'RECEIPTS',        'Receipts'           # receipts
    CONSUMPTIONS    = 'CONSUMPTIONS',    'Consumptions'       # consumptions
    LOSSES          = 'LOSSES',          'Losses'             # losses
    ADJUSTMENTS     = 'ADJUSTMENTS',     'Adjustments'        # net_transfers + net_adjustments
    CLOSING_BALANCE = 'CLOSING_BALANCE', 'Closing Balance'    # closing_balance
    STOCKOUT_DAYS   = 'STOCKOUT_DAYS',   'Stockout Days'      # stockout_days


class DataElementMapping(models.Model):
    """
    Mapping between OpenLMIS product UUID + indicator and DHIS2 data element.
    Source: analytics.fact_stock_monthly.product_id
    """
    openlmis_product_id = models.UUIDField(
        db_index=True,
        help_text="OpenLMIS product UUID (from kafka_orderables.id)"
    )
    indicator = models.CharField(
        max_length=30,
        choices=IndicatorType.choices,
        db_index=True,
    )
    dhis2_data_element_uid = models.CharField(
        max_length=11,
        help_text="DHIS2 Data Element UID"
    )
    dhis2_category_option_combo_uid = models.CharField(
        max_length=11,
        blank=True,
        default='',
        help_text="DHIS2 Category Option Combo UID (optional)"
    )
    dataset = models.ForeignKey(
        'DataSet',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='data_elements'
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'data_element_mappings'
        verbose_name = 'Data Element Mapping'
        verbose_name_plural = 'Data Element Mappings'
        unique_together = [['openlmis_product_id', 'indicator']]
        indexes = [
            models.Index(fields=['openlmis_product_id', 'indicator', 'is_active']),
        ]

    def __str__(self):
        return f"{self.openlmis_product_id} ({self.indicator}) -> {self.dhis2_data_element_uid}"


class PeriodType(models.TextChoices):
    """DHIS2 period types."""
    DAILY     = 'DAILY',     'Daily'
    WEEKLY    = 'WEEKLY',    'Weekly'
    MONTHLY   = 'MONTHLY',   'Monthly'
    QUARTERLY = 'QUARTERLY', 'Quarterly'
    YEARLY    = 'YEARLY',    'Yearly'


class DataSet(models.Model):
    """
    DHIS2 DataSet configuration.
    """
    name = models.CharField(max_length=255, unique=True)
    dhis2_dataset_uid = models.CharField(max_length=11, unique=True)
    period_type = models.CharField(
        max_length=20,
        choices=PeriodType.choices,
        default=PeriodType.MONTHLY
    )
    description = models.TextField(blank=True)
    server = models.ForeignKey(
        DHIS2Server,
        on_delete=models.CASCADE,
        related_name='datasets',
        null=True,
        blank=True
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'datasets'
        verbose_name = 'DataSet'
        verbose_name_plural = 'DataSets'

    def __str__(self):
        return f"{self.name} ({self.dhis2_dataset_uid})"


class SyncSchedule(models.Model):
    """
    Sync schedule configuration.
    """
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    cron_expression = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)
    facilities = models.ManyToManyField(
        FacilityMapping, blank=True, related_name='schedules'
    )
    datasets = models.ManyToManyField(
        DataSet, blank=True, related_name='schedules'
    )
    last_run = models.DateTimeField(null=True, blank=True)
    next_run = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'sync_schedules'
        verbose_name = 'Sync Schedule'
        verbose_name_plural = 'Sync Schedules'

    def __str__(self):
        return f"{self.name} ({'Active' if self.is_active else 'Inactive'})"


class SyncStatus(models.TextChoices):
    """Sync operation status."""
    PENDING = 'PENDING', 'Pending'
    RUNNING = 'RUNNING', 'Running'
    SUCCESS = 'SUCCESS', 'Success'
    PARTIAL = 'PARTIAL', 'Partial Success'
    FAILED  = 'FAILED',  'Failed'


class SyncLog(models.Model):
    """
    Sync operation log for auditing and debugging.
    """
    facility = models.ForeignKey(
        FacilityMapping, on_delete=models.SET_NULL, null=True, related_name='sync_logs'
    )
    period = models.CharField(max_length=10, db_index=True)
    status = models.CharField(max_length=20, choices=SyncStatus.choices, default=SyncStatus.PENDING)
    started_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    records_extracted = models.IntegerField(default=0)
    records_transformed = models.IntegerField(default=0)
    records_loaded = models.IntegerField(default=0)
    records_failed = models.IntegerField(default=0)
    details = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    celery_task_id = models.CharField(max_length=255, blank=True, db_index=True)

    class Meta:
        db_table = 'sync_logs'
        verbose_name = 'Sync Log'
        verbose_name_plural = 'Sync Logs'
        indexes = [
            models.Index(fields=['facility', 'period']),
            models.Index(fields=['status', 'started_at']),
        ]
        ordering = ['-started_at']

    def __str__(self):
        fid = str(self.facility.openlmis_facility_id) if self.facility else 'Unknown'
        return f"{fid} - {self.period} ({self.status})"

    @property
    def duration(self):
        if self.completed_at and self.started_at:
            return self.completed_at - self.started_at
        return None


class AggregatedData(models.Model):
    """
    Stores aggregated stock data before pushing to DHIS2 (audit trail).
    """
    facility = models.ForeignKey(
        FacilityMapping, on_delete=models.CASCADE, related_name='aggregated_data'
    )
    period = models.CharField(max_length=10, db_index=True)
    openlmis_product_id = models.UUIDField(db_index=True)
    indicator = models.CharField(max_length=30, choices=IndicatorType.choices)
    value = models.DecimalField(max_digits=15, decimal_places=2)
    dhis2_data_element_uid = models.CharField(max_length=11)
    dhis2_category_option_combo_uid = models.CharField(max_length=11, blank=True)
    dhis2_org_unit_id = models.CharField(max_length=11)
    is_synced = models.BooleanField(default=False)
    synced_at = models.DateTimeField(null=True, blank=True)
    sync_response = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'aggregated_data'
        verbose_name = 'Aggregated Data'
        verbose_name_plural = 'Aggregated Data'
        unique_together = [['facility', 'period', 'openlmis_product_id', 'indicator']]
        indexes = [
            models.Index(fields=['period', 'is_synced']),
        ]

    def __str__(self):
        return f"{self.facility} - {self.openlmis_product_id} ({self.indicator}): {self.value}"
