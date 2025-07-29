"""
Models package initialization.
"""

from .data_models import (
    Product, 
    InventoryItem, 
    ConsumptionRecord, 
    Alert, 
    Receipt,
    ProductCategory,
    ConsumptionStatus
)
from .config import config

__all__ = [
    'Product',
    'InventoryItem', 
    'ConsumptionRecord',
    'Alert',
    'Receipt',
    'ProductCategory',
    'ConsumptionStatus',
    'config'
]