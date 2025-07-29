"""
Data models for the Smart Expiry & Consumption Tracker.

This module defines the core data structures used throughout the application.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
from enum import Enum


class ProductCategory(Enum):
    """Product categories for classification."""
    FOOD = "food"
    MEDICINE = "medicine"
    TOILETRIES = "toiletries"
    HOUSEHOLD = "household"
    OTHER = "other"


class ConsumptionStatus(Enum):
    """Status of inventory items."""
    AVAILABLE = "available"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    WASTED = "wasted"


@dataclass
class Product:
    """Product data model."""
    product_id: str
    name: str
    category: ProductCategory
    brand: Optional[str] = None
    unit: str = "piece"  # piece, kg, liter, etc.
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "product_id": self.product_id,
            "name": self.name,
            "category": self.category.value,
            "brand": self.brand,
            "unit": self.unit
        }


@dataclass
class InventoryItem:
    """Inventory item data model."""
    item_id: str
    product_id: str
    purchase_date: datetime
    expiry_date: datetime
    quantity: float
    cost: Optional[float] = None
    status: ConsumptionStatus = ConsumptionStatus.AVAILABLE
    location: Optional[str] = None  # fridge, pantry, etc.
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "item_id": self.item_id,
            "product_id": self.product_id,
            "purchase_date": self.purchase_date.isoformat(),
            "expiry_date": self.expiry_date.isoformat(),
            "quantity": self.quantity,
            "cost": self.cost,
            "status": self.status.value,
            "location": self.location
        }
    
    @property
    def days_until_expiry(self) -> int:
        """Calculate days until expiry."""
        return (self.expiry_date - datetime.now()).days
    
    @property
    def is_expired(self) -> bool:
        """Check if item is expired."""
        return datetime.now() > self.expiry_date
    
    @property
    def is_expiring_soon(self, days: int = 3) -> bool:
        """Check if item is expiring within specified days."""
        return 0 <= self.days_until_expiry <= days


@dataclass
class ConsumptionRecord:
    """Consumption tracking data model."""
    record_id: str
    item_id: str
    consumption_date: datetime
    quantity_consumed: float
    user_id: Optional[str] = None
    notes: Optional[str] = None
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "record_id": self.record_id,
            "item_id": self.item_id,
            "consumption_date": self.consumption_date.isoformat(),
            "quantity_consumed": self.quantity_consumed,
            "user_id": self.user_id,
            "notes": self.notes
        }


@dataclass
class Alert:
    """Alert/notification data model."""
    alert_id: str
    item_id: str
    alert_type: str  # expiry_warning, expiry_critical, consumption_reminder
    message: str
    created_date: datetime
    is_read: bool = False
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "alert_id": self.alert_id,
            "item_id": self.item_id,
            "alert_type": self.alert_type,
            "message": self.message,
            "created_date": self.created_date.isoformat(),
            "is_read": self.is_read
        }


@dataclass
class Receipt:
    """Receipt data model for raw data ingestion."""
    receipt_id: str
    store_name: Optional[str]
    purchase_date: datetime
    total_amount: Optional[float]
    raw_text: str  # OCR output or manual entry
    items: Optional[List[dict]] = None  # parsed items
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "receipt_id": self.receipt_id,
            "store_name": self.store_name,
            "purchase_date": self.purchase_date.isoformat(),
            "total_amount": self.total_amount,
            "raw_text": self.raw_text,
            "items": self.items
        }