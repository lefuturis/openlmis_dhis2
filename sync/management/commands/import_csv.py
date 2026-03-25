"""
Management command to import CSV configuration files.
CSV format uses OpenLMIS UUIDs, not codes.
"""
import csv
import uuid
from pathlib import Path
from django.core.management.base import BaseCommand
from loguru import logger

from sync.models import FacilityMapping, DataElementMapping, DataSet, IndicatorType


# Mapping from CSV openlmisAttribute to IndicatorType
ATTRIBUTE_TO_INDICATOR = {
    'beginningBalance':       IndicatorType.OPENING_BALANCE,
    'quantityReceived':       IndicatorType.RECEIPTS,
    'quantityDispensed':      IndicatorType.CONSUMPTIONS,
    'totalLosses':            IndicatorType.LOSSES,
    'adjustments':            IndicatorType.ADJUSTMENTS,
    'closingBalance':         IndicatorType.CLOSING_BALANCE,
    'stockoutDays':           IndicatorType.STOCKOUT_DAYS,
}


class Command(BaseCommand):
    help = 'Import CSV configuration files (UUID-based) into the database'

    def add_arguments(self, parser):
        parser.add_argument('--all',       action='store_true', help='Import all CSV files')
        parser.add_argument('--facilities', action='store_true', help='Import facilities mapping CSV')
        parser.add_argument('--mappings',   action='store_true', help='Import data element mappings CSV')
        parser.add_argument(
            '--data-dir', type=str, default='/app/data',
            help='Directory containing CSV files (default: /app/data)',
        )

    def handle(self, *args, **options):
        data_dir = Path(options['data_dir'])
        if not data_dir.exists():
            self.stderr.write(self.style.ERROR(f'Data directory not found: {data_dir}'))
            return

        import_all = options['all']
        if import_all or options['facilities']:
            self.import_facilities(data_dir)
        if import_all or options['mappings']:
            self.import_mappings(data_dir)
        if not (import_all or options['facilities'] or options['mappings']):
            self.stdout.write(self.style.WARNING(
                'No import option specified. Use --all, --facilities, or --mappings'
            ))

    def import_facilities(self, data_dir: Path):
        """
        Import facility mappings from CSV.
        Expected columns: openlmisId,dhis2OrgUnitId
        """
        csv_file = data_dir / 'facilities.csv'
        if not csv_file.exists():
            self.stderr.write(self.style.WARNING(f'Facilities CSV not found: {csv_file}'))
            return

        self.stdout.write(f'Importing facilities from {csv_file}...')
        created_count = updated_count = skipped_count = 0

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_id       = row.get('openlmisId', '').strip()
                dhis2_uid    = row.get('dhis2OrgUnitId', '').strip()

                if not raw_id or not dhis2_uid:
                    skipped_count += 1
                    continue

                try:
                    facility_uuid = uuid.UUID(raw_id)
                except ValueError:
                    self.stderr.write(self.style.WARNING(f'Invalid UUID skipped: {raw_id}'))
                    skipped_count += 1
                    continue

                facility, created = FacilityMapping.objects.update_or_create(
                    openlmis_facility_id=facility_uuid,
                    defaults={
                        'dhis2_org_unit_id': dhis2_uid,
                        'is_active': True,
                    }
                )
                if created:
                    created_count += 1
                    logger.info(f"Created facility mapping: {facility_uuid} -> {dhis2_uid}")
                else:
                    updated_count += 1
                    logger.info(f"Updated facility mapping: {facility_uuid} -> {dhis2_uid}")

        self.stdout.write(self.style.SUCCESS(
            f'Facilities import complete: {created_count} created, {updated_count} updated, {skipped_count} skipped'
        ))

    def import_mappings(self, data_dir: Path):
        """
        Import data element mappings from CSV.
        Expected columns: productId,openlmisAttribute,dhis2DeId,dhis2CocId
        """
        csv_file = data_dir / 'data_mapping.csv'
        if not csv_file.exists():
            self.stderr.write(self.style.WARNING(f'Data mapping CSV not found: {csv_file}'))
            return

        self.stdout.write(f'Importing data mappings from {csv_file}...')
        created_count = updated_count = skipped_count = 0

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_product_id = row.get('productId', '').strip()
                openlmis_attr  = row.get('openlmisAttribute', '').strip()
                dhis2_de_id    = row.get('dhis2DeId', '').strip()
                dhis2_coc_id   = row.get('dhis2CocId', '').strip()
                dataset_uid    = row.get('datasetId', '').strip()

                if not raw_product_id or not openlmis_attr or not dhis2_de_id:
                    skipped_count += 1
                    continue

                try:
                    product_uuid = uuid.UUID(raw_product_id)
                except ValueError:
                    self.stderr.write(self.style.WARNING(f'Invalid UUID skipped: {raw_product_id}'))
                    skipped_count += 1
                    continue

                indicator = ATTRIBUTE_TO_INDICATOR.get(openlmis_attr)
                if not indicator:
                    self.stderr.write(self.style.WARNING(
                        f'Unknown openlmisAttribute: {openlmis_attr}'
                    ))
                    skipped_count += 1
                    continue

                # Resolve DataSet if provided
                dataset_obj = None
                if dataset_uid:
                    dataset_obj, _ = DataSet.objects.get_or_create(
                        dhis2_dataset_uid=dataset_uid,
                        defaults={'name': dataset_uid, 'is_active': True},
                    )

                mapping, created = DataElementMapping.objects.update_or_create(
                    openlmis_product_id=product_uuid,
                    indicator=indicator,
                    defaults={
                        'dhis2_data_element_uid': dhis2_de_id,
                        'dhis2_category_option_combo_uid': dhis2_coc_id,
                        'dataset': dataset_obj,
                        'is_active': True,
                    }
                )
                if created:
                    created_count += 1
                    logger.info(f"Created mapping: {product_uuid} - {indicator} -> {dhis2_de_id}")
                else:
                    updated_count += 1
                    logger.info(f"Updated mapping: {product_uuid} - {indicator} -> {dhis2_de_id}")

        self.stdout.write(self.style.SUCCESS(
            f'Data mappings import complete: {created_count} created, {updated_count} updated, {skipped_count} skipped'
        ))
