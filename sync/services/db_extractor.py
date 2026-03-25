"""
Database Extractor Service.

Extracts aggregated stock data directly from the OpenLMIS reporting database
view analytics.fact_stock_monthly and maps it to DHIS2 indicators.
"""
import uuid
from typing import List, Dict, Any, Tuple
from loguru import logger
from django.db import connections

from ..models import IndicatorType, DataElementMapping


class DatabaseExtractor:
    """
    Extracts stock data directly from the open_lmis_reporting database.
    Uses analytics.fact_stock_monthly queried by facility_id (UUID).
    """

    def __init__(self):
        self._mapping_cache: Dict[Tuple[str, str], DataElementMapping] = {}
        self._load_mappings()

    def _load_mappings(self):
        """Load data element mappings from database into cache."""
        mappings = DataElementMapping.objects.filter(is_active=True)
        for mapping in mappings:
            key = (str(mapping.openlmis_product_id), mapping.indicator)
            self._mapping_cache[key] = mapping
        logger.debug(f"Loaded {len(self._mapping_cache)} data element mappings")

    # View column → IndicatorType
    FIELDS_TO_INDICATORS = {
        'opening_balance': IndicatorType.OPENING_BALANCE,
        'receipts':        IndicatorType.RECEIPTS,
        'consumptions':    IndicatorType.CONSUMPTIONS,
        'losses':          IndicatorType.LOSSES,
        'adjustments':     IndicatorType.ADJUSTMENTS,
        'closing_balance': IndicatorType.CLOSING_BALANCE,
        'stockout_days':   IndicatorType.STOCKOUT_DAYS,
    }

    def extract_monthly_data(
        self,
        facility_id: str,
        period: str,
        start_date_str: str,  # format 'YYYY-MM-DD'
    ) -> List[Dict[str, Any]]:
        """
        Extract pre-aggregated monthly data from fact_stock_monthly and
        format as DHIS2 data values.

        Args:
            facility_id: OpenLMIS facility UUID (matches analytics.fact_stock_monthly.facility_id)
            period: DHIS2 period (YYYYMM)
            start_date_str: First day of the month as 'YYYY-MM-01'

        Returns:
            List of data value dicts ready to be sent to DHIS2.
        """
        logger.info(
            f"Extracting stock data from analytics.fact_stock_monthly "
            f"for facility_id={facility_id} (period: {period})"
        )

        # Compute adjustments = net_transfers + net_adjustments directly in SQL
        sql = """
            SELECT
                m.product_id::text AS product_id,
                m.opening_balance,
                m.receipts,
                m.consumptions,
                m.losses,
                (COALESCE(m.net_transfers, 0) + COALESCE(m.net_adjustments, 0)) AS adjustments,
                m.closing_balance,
                m.stockout_days
            FROM analytics.fact_stock_monthly m
            WHERE m.facility_id = %s::uuid
              AND m.month_date = %s
        """

        try:
            with connections['openlmis_reporting'].cursor() as cursor:
                cursor.execute(sql, [facility_id, start_date_str])
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                monthly_data = [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to query database for facility {facility_id}: {e}")
            raise

        logger.info(f"Found {len(monthly_data)} product records for facility {facility_id}")

        data_values = []
        missing_mappings = set()

        for record in monthly_data:
            product_id = str(record['product_id'])

            for field, indicator in self.FIELDS_TO_INDICATORS.items():
                value = record.get(field)
                if value is None:
                    continue

                mapping_key = (product_id, indicator)
                mapping = self._mapping_cache.get(mapping_key)

                if not mapping:
                    missing_mappings.add(f"{product_id}:{indicator}")
                    continue

                data_value = {
                    'product_id': product_id,
                    'indicator': indicator,
                    'value': int(abs(float(value))),
                    'dhis2_data_element_uid': mapping.dhis2_data_element_uid,
                    'dhis2_category_option_combo_uid': mapping.dhis2_category_option_combo_uid,
                    'period': period,
                }
                data_values.append(data_value)
                logger.debug(f"Mapped product {product_id} - {indicator}: {float(value)}")

        if missing_mappings:
            for missing in missing_mappings:
                pid, ind = missing.split(':', 1)
                logger.warning(f"Mapping missing for product_id='{pid}' indicator='{ind}'")

        logger.info(f"Generated {len(data_values)} DHIS2 data values for facility {facility_id}")
        return data_values
