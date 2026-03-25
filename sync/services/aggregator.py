"""
Stock Movement Aggregator.

Aggregates raw stock movements into monthly indicators for DHIS2.
"""
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from loguru import logger

from ..models import DataElementMapping, IndicatorType


class StockAggregator:
    """
    Aggregates stock movements from OpenLMIS into DHIS2-compatible data values.
    
    Uses Pandas for efficient data manipulation and aggregation.
    """
    
    # Mapping from OpenLMIS reason categories to our indicator types
    REASON_CATEGORY_MAPPING = {
        # OpenLMIS reason categories
        'TRANSFER': {
            'CREDIT': IndicatorType.RECEIVED,
            'DEBIT': IndicatorType.ISSUED,
        },
        'ADJUSTMENT': {
            'CREDIT': IndicatorType.ADJUSTMENT_POSITIVE,
            'DEBIT': IndicatorType.ADJUSTMENT_NEGATIVE,
        },
        'PHYSICAL_INVENTORY': {
            'CREDIT': IndicatorType.ADJUSTMENT_POSITIVE,
            'DEBIT': IndicatorType.ADJUSTMENT_NEGATIVE,
        },
    }
    
    # Common OpenLMIS reason names mapping
    REASON_NAME_MAPPING = {
        'received': IndicatorType.RECEIVED,
        'receipt': IndicatorType.RECEIVED,
        'receive': IndicatorType.RECEIVED,
        'issued': IndicatorType.ISSUED,
        'issue': IndicatorType.ISSUED,
        'consumed': IndicatorType.ISSUED,
        'consumption': IndicatorType.ISSUED,
        'expired': IndicatorType.EXPIRED,
        'expiry': IndicatorType.EXPIRED,
        'damaged': IndicatorType.DAMAGED,
        'damage': IndicatorType.DAMAGED,
        'lost': IndicatorType.LOST,
        'loss': IndicatorType.LOST,
        'transfer in': IndicatorType.RECEIVED,
        'transfer out': IndicatorType.ISSUED,
    }
    
    def __init__(self):
        """Initialize the aggregator."""
        self._mapping_cache: Dict[Tuple[str, str], DataElementMapping] = {}
        self._load_mappings()
    
    def _load_mappings(self):
        """Load data element mappings from database."""
        mappings = DataElementMapping.objects.filter(is_active=True)
        for mapping in mappings:
            key = (mapping.openlmis_product_code, mapping.indicator)
            self._mapping_cache[key] = mapping
        logger.debug(f"Loaded {len(self._mapping_cache)} data element mappings")
    
    def _determine_indicator_type(self, movement: Dict[str, Any]) -> Optional[str]:
        """
        Determine the indicator type from a stock movement.
        
        Args:
            movement: Stock movement/line item from OpenLMIS
            
        Returns:
            Indicator type string or None
        """
        reason = movement.get('reason', {})
        reason_type = reason.get('reasonType', '')
        reason_category = reason.get('reasonCategory', '')
        reason_name = reason.get('name', '').lower()
        
        # First try to map by reason name
        for key, indicator in self.REASON_NAME_MAPPING.items():
            if key in reason_name:
                return indicator
        
        # Then try by reason category and type
        if reason_category in self.REASON_CATEGORY_MAPPING:
            category_mapping = self.REASON_CATEGORY_MAPPING[reason_category]
            if reason_type in category_mapping:
                return category_mapping[reason_type]
        
        # Default based on reason type only
        if reason_type == 'CREDIT':
            return IndicatorType.RECEIVED
        elif reason_type == 'DEBIT':
            return IndicatorType.ISSUED
        
        logger.warning(f"Could not determine indicator type for reason: {reason}")
        return None
    
    def aggregate(
        self,
        movements: List[Dict[str, Any]],
        period: str,
        facility_code: str
    ) -> List[Dict[str, Any]]:
        """
        Aggregate stock movements into DHIS2 data values.
        
        Args:
            movements: List of stock movements from OpenLMIS
            period: DHIS2 period format (YYYYMM)
            facility_code: OpenLMIS facility code
            
        Returns:
            List of aggregated data values ready for DHIS2
        """
        if not movements:
            logger.warning(f"No movements to aggregate for {facility_code}")
            return []
        
        logger.info(f"Aggregating {len(movements)} movements for {facility_code} (period: {period})")
        
        # Convert to DataFrame for efficient aggregation
        df = pd.DataFrame(movements)
        
        # Add indicator type column
        df['indicatorType'] = df.apply(
            lambda row: self._determine_indicator_type(row.to_dict()),
            axis=1
        )
        
        # Remove rows without valid indicator type
        df = df.dropna(subset=['indicatorType'])
        
        # Get quantity (handle both positive and negative values)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        # Aggregate by product and indicator type
        aggregated = df.groupby(['productCode', 'indicatorType']).agg({
            'quantity': 'sum',
            'productName': 'first'
        }).reset_index()
        
        # Convert to DHIS2 data values
        data_values = []
        missing_mappings = set()
        
        # Track adjustments per product for net calculation
        product_adjustments = {}
        product_names = {}
        
        for _, row in aggregated.iterrows():
            product_code = row['productCode']
            indicator = row['indicatorType']
            value = row['quantity']
            
            # Track product names for later use
            product_names[product_code] = row['productName']
            
            # Handle adjustments - accumulate for net calculation
            if indicator == IndicatorType.ADJUSTMENT_POSITIVE:
                if product_code not in product_adjustments:
                    product_adjustments[product_code] = 0
                product_adjustments[product_code] += abs(value)
                continue  # Don't add individual adjustment, will add net later
            elif indicator == IndicatorType.ADJUSTMENT_NEGATIVE:
                if product_code not in product_adjustments:
                    product_adjustments[product_code] = 0
                product_adjustments[product_code] -= abs(value)
                continue  # Don't add individual adjustment, will add net later
            
            # For non-adjustment indicators, use absolute value
            value = abs(value)
            
            # Look up the DHIS2 mapping
            mapping_key = (product_code, indicator)
            mapping = self._mapping_cache.get(mapping_key)
            
            if not mapping:
                missing_mappings.add(f"{product_code}:{indicator}")
                continue
            
            data_value = {
                'product_code': product_code,
                'product_name': row['productName'],
                'indicator': indicator,
                'value': float(value),
                'dhis2_data_element_uid': mapping.dhis2_data_element_uid,
                'dhis2_category_option_combo_uid': mapping.dhis2_category_option_combo_uid,
                'period': period,
            }
            
            data_values.append(data_value)
            
            logger.debug(f"Aggregated {product_code} - {indicator}: {value}")
        
        # Add net adjustments (TOTAL_ADJUSTMENT) for each product
        for product_code, net_adjustment in product_adjustments.items():
            mapping_key = (product_code, IndicatorType.TOTAL_ADJUSTMENT)
            mapping = self._mapping_cache.get(mapping_key)
            
            if not mapping:
                missing_mappings.add(f"{product_code}:{IndicatorType.TOTAL_ADJUSTMENT}")
                continue
            
            # Value can be positive or negative
            data_value = {
                'product_code': product_code,
                'product_name': product_names.get(product_code, ''),
                'indicator': IndicatorType.TOTAL_ADJUSTMENT,
                'value': float(net_adjustment),  # Keep sign (positive or negative)
                'dhis2_data_element_uid': mapping.dhis2_data_element_uid,
                'dhis2_category_option_combo_uid': mapping.dhis2_category_option_combo_uid,
                'period': period,
            }
            
            data_values.append(data_value)
            logger.debug(f"Net adjustment for {product_code}: {net_adjustment}")
        
        # Log missing mappings
        if missing_mappings:
            for missing in missing_mappings:
                product, indicator = missing.split(':')
                logger.error(f"Mapping missing for Product '{product}' and indicator '{indicator}' in data_elements.csv")
        
        logger.info(f"Generated {len(data_values)} aggregated data values for {facility_code}")
        return data_values
    
    def calculate_opening_balance(
        self,
        movements: List[Dict[str, Any]],
        product_code: str,
        start_date: datetime
    ) -> Optional[int]:
        """
        Calculate opening balance for a product at start of period.
        
        Args:
            movements: List of stock movements sorted by date
            product_code: Product code
            start_date: Period start date
            
        Returns:
            Opening balance or None
        """
        # Filter movements for this product before start date
        product_movements = [
            m for m in movements 
            if m.get('productCode') == product_code
        ]
        
        if not product_movements:
            return None
        
        # Sort by date (oldest first)
        product_movements.sort(key=lambda x: x.get('occurredDate', ''))
        
        # Get stock on hand from the first movement
        for movement in product_movements:
            soh = movement.get('stockOnHand')
            if soh is not None:
                return int(soh)
        
        return None
    
    def calculate_closing_balance(
        self,
        movements: List[Dict[str, Any]],
        product_code: str,
        end_date: datetime
    ) -> Optional[int]:
        """
        Calculate closing balance for a product at end of period.
        
        Args:
            movements: List of stock movements
            product_code: Product code  
            end_date: Period end date
            
        Returns:
            Closing balance or None
        """
        # Filter movements for this product
        product_movements = [
            m for m in movements 
            if m.get('productCode') == product_code
        ]
        
        if not product_movements:
            return None
        
        # Sort by date (newest first)
        product_movements.sort(key=lambda x: x.get('occurredDate', ''), reverse=True)
        
        # Get stock on hand from the last movement
        for movement in product_movements:
            soh = movement.get('stockOnHand')
            if soh is not None:
                return int(soh)
        
        return None
    
    def aggregate_with_balances(
        self,
        movements: List[Dict[str, Any]],
        period: str,
        facility_code: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Aggregate stock movements including opening and closing balances.
        
        Args:
            movements: List of stock movements
            period: DHIS2 period format
            facility_code: Facility code
            start_date: Period start date
            end_date: Period end date
            
        Returns:
            List of aggregated data values
        """
        # Get regular aggregations first
        data_values = self.aggregate(movements, period, facility_code)
        
        # Get unique products
        products = set(m.get('productCode') for m in movements if m.get('productCode'))
        
        # Add opening and closing balances
        for product_code in products:
            # Opening balance
            opening = self.calculate_opening_balance(movements, product_code, start_date)
            if opening is not None:
                mapping_key = (product_code, IndicatorType.OPENING_BALANCE)
                mapping = self._mapping_cache.get(mapping_key)
                if mapping:
                    data_values.append({
                        'product_code': product_code,
                        'indicator': IndicatorType.OPENING_BALANCE,
                        'value': float(opening),
                        'dhis2_data_element_uid': mapping.dhis2_data_element_uid,
                        'dhis2_category_option_combo_uid': mapping.dhis2_category_option_combo_uid,
                        'period': period,
                    })
                    logger.debug(f"Opening balance for {product_code}: {opening}")
            
            # Closing balance
            closing = self.calculate_closing_balance(movements, product_code, end_date)
            if closing is not None:
                mapping_key = (product_code, IndicatorType.CLOSING_BALANCE)
                mapping = self._mapping_cache.get(mapping_key)
                if mapping:
                    data_values.append({
                        'product_code': product_code,
                        'indicator': IndicatorType.CLOSING_BALANCE,
                        'value': float(closing),
                        'dhis2_data_element_uid': mapping.dhis2_data_element_uid,
                        'dhis2_category_option_combo_uid': mapping.dhis2_category_option_combo_uid,
                        'period': period,
                    })
                    logger.debug(f"Closing balance for {product_code}: {closing}")
        
        return data_values
