"""
Bronze layer data ingestion for Smart Expiry & Consumption Tracker.

This module handles raw data ingestion from various sources into the bronze layer.
"""

import json
import uuid
from datetime import datetime
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.models.config import config


class BronzeDataIngestion:
    """Handle raw data ingestion into bronze layer."""
    
    def __init__(self):
        """Initialize Spark session and configuration."""
        self.spark = SparkSession.builder \
            .appName(config.SPARK_APP_NAME + "_Bronze") \
            .master(config.SPARK_MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Ensure bronze directory exists
        os.makedirs(config.BRONZE_DIR, exist_ok=True)
    
    def ingest_receipt_data(self, receipt_data: List[Dict[str, Any]]) -> DataFrame:
        """
        Ingest raw receipt data into bronze layer.
        
        Args:
            receipt_data: List of receipt dictionaries
            
        Returns:
            DataFrame: Processed receipt DataFrame
        """
        # Define schema for receipts
        receipt_schema = StructType([
            StructField("receipt_id", StringType(), False),
            StructField("store_name", StringType(), True),
            StructField("purchase_date", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("raw_text", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        # Add metadata and standardize format
        processed_receipts = []
        for receipt in receipt_data:
            processed_receipt = {
                "receipt_id": receipt.get("receipt_id", str(uuid.uuid4())),
                "store_name": receipt.get("store_name"),
                "purchase_date": self._parse_datetime(receipt.get("purchase_date")),
                "total_amount": receipt.get("total_amount"),
                "raw_text": receipt.get("raw_text", ""),
                "created_at": datetime.now()
            }
            processed_receipts.append(processed_receipt)
        
        # Create DataFrame
        df = self.spark.createDataFrame(processed_receipts, receipt_schema)
        
        # Save to bronze layer
        output_path = os.path.join(config.BRONZE_DIR, "receipts")
        df.write \
          .mode("append") \
          .partitionBy("purchase_date") \
          .parquet(output_path)
        
        return df
    
    def ingest_inventory_data(self, inventory_data: List[Dict[str, Any]]) -> DataFrame:
        """
        Ingest raw inventory data into bronze layer.
        
        Args:
            inventory_data: List of inventory item dictionaries
            
        Returns:
            DataFrame: Processed inventory DataFrame
        """
        # Define schema for inventory
        inventory_schema = StructType([
            StructField("entry_id", StringType(), False),
            StructField("product_name", StringType(), False),
            StructField("brand", StringType(), True),
            StructField("category", StringType(), True),
            StructField("purchase_date", TimestampType(), False),
            StructField("expiry_date", TimestampType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("unit", StringType(), True),
            StructField("cost", DoubleType(), True),
            StructField("location", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        # Process inventory data
        processed_inventory = []
        for item in inventory_data:
            processed_item = {
                "entry_id": item.get("entry_id", str(uuid.uuid4())),
                "product_name": item.get("product_name", ""),
                "brand": item.get("brand"),
                "category": item.get("category", "other"),
                "purchase_date": self._parse_datetime(item.get("purchase_date")),
                "expiry_date": self._parse_datetime(item.get("expiry_date")),
                "quantity": item.get("quantity", 1.0),
                "unit": item.get("unit", "piece"),
                "cost": item.get("cost"),
                "location": item.get("location"),
                "created_at": datetime.now()
            }
            processed_inventory.append(processed_item)
        
        # Create DataFrame
        df = self.spark.createDataFrame(processed_inventory, inventory_schema)
        
        # Save to bronze layer
        output_path = os.path.join(config.BRONZE_DIR, "inventory")
        df.write \
          .mode("append") \
          .partitionBy("category") \
          .parquet(output_path)
        
        return df
    
    def ingest_consumption_data(self, consumption_data: List[Dict[str, Any]]) -> DataFrame:
        """
        Ingest raw consumption data into bronze layer.
        
        Args:
            consumption_data: List of consumption record dictionaries
            
        Returns:
            DataFrame: Processed consumption DataFrame
        """
        # Define schema for consumption
        consumption_schema = StructType([
            StructField("log_id", StringType(), False),
            StructField("item_identifier", StringType(), False),
            StructField("consumption_date", TimestampType(), False),
            StructField("quantity_consumed", DoubleType(), False),
            StructField("user_id", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        # Process consumption data
        processed_consumption = []
        for record in consumption_data:
            processed_record = {
                "log_id": record.get("log_id", str(uuid.uuid4())),
                "item_identifier": record.get("item_identifier", ""),
                "consumption_date": self._parse_datetime(record.get("consumption_date")),
                "quantity_consumed": record.get("quantity_consumed", 0.0),
                "user_id": record.get("user_id"),
                "notes": record.get("notes"),
                "created_at": datetime.now()
            }
            processed_consumption.append(processed_record)
        
        # Create DataFrame
        df = self.spark.createDataFrame(processed_consumption, consumption_schema)
        
        # Save to bronze layer
        output_path = os.path.join(config.BRONZE_DIR, "consumption")
        df.write \
          .mode("append") \
          .partitionBy("consumption_date") \
          .parquet(output_path)
        
        return df
    
    def _parse_datetime(self, date_str: Any) -> datetime:
        """Parse datetime from various formats."""
        if isinstance(date_str, datetime):
            return date_str
        
        if isinstance(date_str, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                try:
                    # Try common formats
                    for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"]:
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
                    # Default to current time if parsing fails
                    return datetime.now()
                except Exception:
                    return datetime.now()
        
        return datetime.now()
    
    def load_from_json(self, file_path: str, data_type: str) -> DataFrame:
        """
        Load data from JSON file and ingest into bronze layer.
        
        Args:
            file_path: Path to JSON file
            data_type: Type of data (receipts, inventory, consumption)
            
        Returns:
            DataFrame: Processed DataFrame
        """
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if data_type == "receipts":
            return self.ingest_receipt_data(data)
        elif data_type == "inventory":
            return self.ingest_inventory_data(data)
        elif data_type == "consumption":
            return self.ingest_consumption_data(data)
        else:
            raise ValueError(f"Unknown data type: {data_type}")
    
    def close(self):
        """Close Spark session."""
        self.spark.stop()


if __name__ == "__main__":
    # Example usage
    ingestion = BronzeDataIngestion()
    
    # Example data ingestion
    sample_inventory = [
        {
            "product_name": "Milk",
            "brand": "Local Dairy",
            "category": "food",
            "purchase_date": "2024-01-15",
            "expiry_date": "2024-01-22",
            "quantity": 1.0,
            "unit": "liter",
            "cost": 3.50,
            "location": "fridge"
        }
    ]
    
    df = ingestion.ingest_inventory_data(sample_inventory)
    print(f"Ingested {df.count()} inventory records")
    
    ingestion.close()