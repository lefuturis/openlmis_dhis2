"""
Sync Manager - Outil CLI pour la gestion OpenLMIS <-> DHIS2

Modes d'utilisation:
    python manage.py sync_manager --action config                    # Importer les CSV
    python manage.py sync_manager --action sync --period 202510      # Sync manuel
    python manage.py sync_manager --action sync --auto               # Sync auto (mois précédent)
"""
import csv
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import transaction
from loguru import logger

from sync.models import (
    FacilityMapping, DataElementMapping, DataSet, IndicatorType, PeriodType
)
from sync.tasks import sync_all_facilities_task


# Mapping OpenLMIS attribute names to IndicatorType
OPENLMIS_ATTRIBUTE_MAPPING = {
    'beginningBalance': IndicatorType.OPENING_BALANCE,
    'quantityReceived': IndicatorType.RECEIPTS,
    'quantityDispensed': IndicatorType.CONSUMPTIONS,
    'stockOnHand': IndicatorType.CLOSING_BALANCE,
    'totalConsumedQuantity': IndicatorType.CONSUMPTIONS,
    'totalReceivedQuantity': IndicatorType.RECEIPTS,
    'adjustments': IndicatorType.ADJUSTMENTS,
    'totalLossesAndAdjustments': IndicatorType.ADJUSTMENTS,
    'positiveAdjustment': IndicatorType.ADJUSTMENTS,
    'negativeAdjustment': IndicatorType.ADJUSTMENTS,
    'closingBalance': IndicatorType.CLOSING_BALANCE,
    'openingBalance': IndicatorType.OPENING_BALANCE,
}


class Command(BaseCommand):
    help = 'Outil de gestion OpenLMIS <-> DHIS2 (import config, déclenchement sync)'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--action',
            type=str,
            choices=['config', 'sync'],
            required=True,
            help="'config' pour importer les CSV, 'sync' pour lancer la synchronisation"
        )
        parser.add_argument(
            '--period',
            type=str,
            help="Période à synchroniser (Format YYYYMM). Ex: 202510"
        )
        parser.add_argument(
            '--auto',
            action='store_true',
            help="Si utilisé avec sync, calcule automatiquement le mois précédent"
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help="Afficher ce qui serait fait sans appliquer les changements"
        )
        parser.add_argument(
            '--data-dir',
            type=str,
            default=None,
            help="Répertoire contenant les fichiers CSV (défaut: project/data/)"
        )
    
    def handle(self, *args, **options):
        action = options['action']
        dry_run = options['dry_run']
        
        if dry_run:
            self.stdout.write(self.style.WARNING("🔍 MODE DRY-RUN - Aucun changement ne sera appliqué"))
        
        if action == 'config':
            self._push_config(options, dry_run)
        elif action == 'sync':
            self._trigger_sync(options, dry_run)
    
    def _get_previous_month(self) -> str:
        """Retourne le mois précédent au format YYYYMM"""
        last_month = datetime.now() - relativedelta(months=1)
        return last_month.strftime("%Y%m")
    
    def _read_csv(self, filepath: Path) -> list:
        """Lire un fichier CSV et retourner une liste de dictionnaires."""
        if not filepath.exists():
            raise CommandError(f"❌ Fichier non trouvé: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            return list(reader)
    
    def _push_config(self, options, dry_run: bool):
        """Importer les configurations depuis les CSV simplifiés."""
        data_dir = Path(options['data_dir']) if options['data_dir'] else settings.DATA_DIR
        
        self.stdout.write(f"\n🛠️  Chargement des configurations depuis {data_dir}")
        
        # Import des facilities
        self._import_facilities(data_dir, dry_run)
        
        # Import des data mappings
        self._import_data_mappings(data_dir, dry_run)
        
        self.stdout.write(self.style.SUCCESS("\n✅ Configuration importée avec succès!"))
    
    @transaction.atomic
    def _import_facilities(self, data_dir: Path, dry_run: bool):
        """Importer facilities.csv"""
        filepath = data_dir / 'facilities.csv'
        rows = self._read_csv(filepath)
        
        self.stdout.write(f"\n📍 Import des structures depuis {filepath.name}...")
        
        created = 0
        updated = 0
        
        for row in rows:
            openlmis_code = row.get('openlmisCode', '').strip()
            dhis2_org_unit = row.get('dhis2OrgUnitId', '').strip()
            
            if not openlmis_code or not dhis2_org_unit:
                continue
            
            if dry_run:
                self.stdout.write(f"  → Créer/Mettre à jour: {openlmis_code} -> {dhis2_org_unit}")
            else:
                obj, was_created = FacilityMapping.objects.update_or_create(
                    openlmis_facility_code=openlmis_code,
                    defaults={
                        'dhis2_org_unit_id': dhis2_org_unit,
                        'is_active': True,
                    }
                )
                if was_created:
                    created += 1
                else:
                    updated += 1
        
        self.stdout.write(
            self.style.SUCCESS(f"  📊 Structures: {created} créées, {updated} mises à jour")
        )
    
    @transaction.atomic
    def _import_data_mappings(self, data_dir: Path, dry_run: bool):
        """Importer data_mapping.csv"""
        filepath = data_dir / 'data_mapping.csv'
        rows = self._read_csv(filepath)
        
        self.stdout.write(f"\n📊 Import des mappings de données depuis {filepath.name}...")
        
        # Track unique datasets to create
        datasets_seen = {}
        elements_created = 0
        elements_updated = 0
        elements_skipped = 0
        
        for row in rows:
            program_code = row.get('programCode', '').strip()
            dataset_id = row.get('dataSetId', '').strip()
            product_code = row.get('productCode', '').strip()
            openlmis_attr = row.get('openlmisAttribute', '').strip()
            dhis2_de_id = row.get('dhis2DeId', '').strip()
            dhis2_coc_id = row.get('dhis2CocId', '').strip()
            description = row.get('desc', '').strip()
            
            if not product_code or not openlmis_attr or not dhis2_de_id:
                elements_skipped += 1
                continue
            
            # Map OpenLMIS attribute to IndicatorType
            indicator = OPENLMIS_ATTRIBUTE_MAPPING.get(openlmis_attr)
            if not indicator:
                self.stdout.write(
                    self.style.WARNING(f"  ⚠️  Attribut inconnu: {openlmis_attr}")
                )
                elements_skipped += 1
                continue
            
            # Create/get DataSet if not already processed
            dataset = None
            if dataset_id and dataset_id not in datasets_seen:
                dataset_name = f"{program_code}_{dataset_id}" if program_code else dataset_id
                
                if not dry_run:
                    dataset, _ = DataSet.objects.update_or_create(
                        dhis2_dataset_uid=dataset_id,
                        defaults={
                            'name': dataset_name,
                            'period_type': PeriodType.MONTHLY,
                            'is_active': True,
                        }
                    )
                datasets_seen[dataset_id] = dataset
            elif dataset_id:
                dataset = datasets_seen.get(dataset_id)
            
            if dry_run:
                self.stdout.write(
                    f"  → {product_code} ({openlmis_attr}) -> {dhis2_de_id}"
                )
            else:
                obj, was_created = DataElementMapping.objects.update_or_create(
                    openlmis_product_code=product_code,
                    indicator=indicator.value,
                    defaults={
                        'dhis2_data_element_uid': dhis2_de_id,
                        'dhis2_category_option_combo_uid': dhis2_coc_id,
                        'dhis2_data_element_name': description,
                        'dataset': dataset,
                        'is_active': True,
                    }
                )
                if was_created:
                    elements_created += 1
                else:
                    elements_updated += 1
        
        self.stdout.write(
            self.style.SUCCESS(
                f"  📊 DataSets: {len(datasets_seen)} traités"
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"  📊 Éléments: {elements_created} créés, {elements_updated} mis à jour, {elements_skipped} ignorés"
            )
        )
    
    def _trigger_sync(self, options, dry_run: bool):
        """Déclencher la synchronisation."""
        period = options.get('period')
        auto = options.get('auto')
        
        # Déterminer la période cible
        if period:
            target_period = period
        elif auto:
            target_period = self._get_previous_month()
            self.stdout.write(
                self.style.HTTP_INFO(f"ℹ️  Mode Auto: Période calculée = {target_period}")
            )
        else:
            raise CommandError("❌ Vous devez spécifier --period OU --auto")
        
        # Validation du format de période
        if len(target_period) != 6 or not target_period.isdigit():
            raise CommandError(f"❌ Format de période invalide: {target_period}. Attendu: YYYYMM")
        
        self.stdout.write(f"\n🔄 Synchronisation pour la période: {target_period}")
        
        if dry_run:
            # Afficher les facilities qui seraient synchronisées
            facilities = FacilityMapping.objects.filter(is_active=True)
            self.stdout.write(f"\n📍 {facilities.count()} structures actives seraient synchronisées:")
            for f in facilities[:10]:  # Limit display
                self.stdout.write(f"  → {f.openlmis_facility_code} -> {f.dhis2_org_unit_id}")
            if facilities.count() > 10:
                self.stdout.write(f"  ... et {facilities.count() - 10} autres")
        else:
            # Déclencher la task Celery
            task = sync_all_facilities_task.delay(period=target_period)
            
            self.stdout.write(
                self.style.SUCCESS(f"\n🚀 Synchronisation déclenchée!")
            )
            self.stdout.write(f"   Task ID: {task.id}")
            self.stdout.write("   Surveillez la progression avec Flower ou les logs Celery.")
