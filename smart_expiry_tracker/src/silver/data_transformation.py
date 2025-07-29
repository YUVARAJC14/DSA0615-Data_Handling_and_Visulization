"""
Silver layer data processing for Smart Expiry & Consumption Tracker.

This module handles data cleaning, transformation, and validation.
"""

import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, trim, lower, regexp_replace, 
    to_timestamp, current_timestamp, lit, udf, coalesce, concat,
    date_format, monotonically_increasing_id, substring
)
from pyspark.sql.types import StringType, BooleanType
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.models.config import config


class SilverDataProcessor:
    """Handle data cleaning and transformation for silver layer."""
    
    def __init__(self):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(config.SPARK_APP_NAME + "_Silver") \
            .master(config.SPARK_MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Ensure silver directory exists
        os.makedirs(config.SILVER_DIR, exist_ok=True)
        
        # Register UDFs
        self.register_udfs()
    
    def register_udfs(self):
        """Register user-defined functions."""
        
        @udf(returnType=StringType())
        def generate_product_id(name, brand, category):
            """Generate a unique product ID."""
            base_string = f"{name}_{brand}_{category}".lower().replace(" ", "_")
            # Remove special characters
            clean_string = ''.join(c for c in base_string if c.isalnum() or c == '_')
            return clean_string[:50]  # Limit length
        
        @udf(returnType=BooleanType())
        def is_valid_expiry_date(purchase_date, expiry_date):
            """Check if expiry date is valid (after purchase date)."""
            if purchase_date and expiry_date:
                return expiry_date > purchase_date
            return False
        
        self.generate_product_id_udf = generate_product_id
        self.is_valid_expiry_date_udf = is_valid_expiry_date
    
    def process_products(self, inventory_df: DataFrame) -> DataFrame:
        """
        Extract and clean products data from inventory.
        
        Args:
            inventory_df: Raw inventory DataFrame from bronze layer
            
        Returns:
            DataFrame: Cleaned products DataFrame
        """
        # Clean and standardize product data
        products_df = inventory_df.select(
            trim(lower(col("product_name"))).alias("name"),
            trim(col("brand")).alias("brand"),
            trim(lower(col("category"))).alias("category"),
            col("unit")
        ).distinct()
        
        # Remove rows with null or empty product names
        products_df = products_df.filter(
            col("name").isNotNull() & 
            (col("name") != "") & 
            (col("name") != "null")
        )
        
        # Generate product IDs
        products_df = products_df.withColumn(
            "product_id",
            self.generate_product_id_udf(col("name"), col("brand"), col("category"))
        )
        
        # Standardize categories
        products_df = products_df.withColumn(
            "category",
            when(col("category").isin(["food", "dairy", "meat", "vegetables", "fruits", "snacks"]), "food")
            .when(col("category").isin(["medicine", "drugs", "pharmacy", "pills"]), "medicine")
            .when(col("category").isin(["toiletries", "cosmetics", "personal_care", "hygiene"]), "toiletries")
            .when(col("category").isin(["household", "cleaning", "kitchen", "home"]), "household")
            .otherwise("other")
        )
        
        # Add metadata
        products_df = products_df.withColumn("created_at", current_timestamp()) \
                                .withColumn("updated_at", current_timestamp())
        
        # Reorder columns
        products_df = products_df.select(
            "product_id", "name", "category", "brand", "unit", "created_at", "updated_at"
        )
        
        # Save to silver layer
        output_path = os.path.join(config.SILVER_DIR, "products")
        products_df.write \
                   .mode("overwrite") \
                   .parquet(output_path)
        
        return products_df
    
    def process_inventory_items(self, inventory_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """
        Clean and process inventory items.
        
        Args:
            inventory_df: Raw inventory DataFrame from bronze layer
            products_df: Cleaned products DataFrame
            
        Returns:
            DataFrame: Cleaned inventory items DataFrame
        """
        # Clean inventory data
        cleaned_inventory = inventory_df.withColumn(
            "product_name_clean", trim(lower(col("product_name")))
        ).withColumn(
            "brand_clean", trim(col("brand"))
        ).withColumn(
            "category_clean", trim(lower(col("category")))
        )
        
        # Generate matching product IDs
        cleaned_inventory = cleaned_inventory.withColumn(
            "product_id_match",
            self.generate_product_id_udf(col("product_name_clean"), col("brand_clean"), col("category_clean"))
        )
        
        # Join with products to get product_id
        inventory_items = cleaned_inventory.join(
            products_df.select("product_id", "name", "brand", "category"),
            cleaned_inventory.product_id_match == products_df.product_id,
            "inner"
        )
        
        # Generate item IDs
        inventory_items = inventory_items.withColumn(
            "item_id",
            concat(lit("item_"), 
                   substring(regexp_replace(col("product_id"), "[^a-zA-Z0-9]", ""), 1, 10),
                   lit("_"),
                   date_format(col("purchase_date"), "yyyyMMdd"),
                   lit("_"),
                   monotonically_increasing_id().cast("string"))
        )
        
        # Validate dates
        inventory_items = inventory_items.filter(
            col("purchase_date").isNotNull() & 
            col("expiry_date").isNotNull() &
            self.is_valid_expiry_date_udf(col("purchase_date"), col("expiry_date"))
        )
        
        # Clean numeric fields
        inventory_items = inventory_items.withColumn(
            "quantity",
            when(col("quantity").isNull() | isnan(col("quantity")), 1.0)
            .otherwise(col("quantity"))
        ).withColumn(
            "cost",
            when(col("cost") <= 0, None)
            .otherwise(col("cost"))
        )
        
        # Set default status
        inventory_items = inventory_items.withColumn("status", lit("available"))
        
        # Add metadata
        inventory_items = inventory_items.withColumn("created_at", current_timestamp()) \
                                       .withColumn("updated_at", current_timestamp())
        
        # Select final columns
        final_inventory = inventory_items.select(
            "item_id",
            "product_id",
            "purchase_date",
            "expiry_date", 
            "quantity",
            "cost",
            "status",
            "location",
            "created_at",
            "updated_at"
        )
        
        # Save to silver layer
        output_path = os.path.join(config.SILVER_DIR, "inventory_items")
        final_inventory.write \
                      .mode("append") \
                      .partitionBy("status") \
                      .parquet(output_path)
        
        return final_inventory
    
    def process_consumption_records(self, consumption_df: DataFrame, inventory_df: DataFrame) -> DataFrame:
        """
        Clean and process consumption records.
        
        Args:
            consumption_df: Raw consumption DataFrame from bronze layer
            inventory_df: Cleaned inventory DataFrame
            
        Returns:
            DataFrame: Cleaned consumption records DataFrame
        """
        # Clean consumption data
        cleaned_consumption = consumption_df.filter(
            col("consumption_date").isNotNull() &
            col("quantity_consumed").isNotNull() &
            (col("quantity_consumed") > 0)
        )
        
        # Match with inventory items (simplified matching by item_identifier)
        # In a real scenario, this would involve more sophisticated matching logic
        consumption_records = cleaned_consumption.withColumn(
            "record_id",
            concat(lit("cons_"), 
                   date_format(col("consumption_date"), "yyyyMMdd"),
                   lit("_"),
                   monotonically_increasing_id().cast("string"))
        )
        
        # For demo purposes, create mock item_id mapping
        # In production, this would match with actual inventory items
        consumption_records = consumption_records.withColumn(
            "item_id",
            concat(lit("item_"), regexp_replace(col("item_identifier"), "[^a-zA-Z0-9]", "_"))
        )
        
        # Add metadata
        consumption_records = consumption_records.withColumn("created_at", current_timestamp())
        
        # Select final columns
        final_consumption = consumption_records.select(
            "record_id",
            "item_id",
            "consumption_date",
            "quantity_consumed",
            "user_id",
            "notes",
            "created_at"
        )
        
        # Save to silver layer
        output_path = os.path.join(config.SILVER_DIR, "consumption_records")
        final_consumption.write \
                         .mode("append") \
                         .partitionBy("consumption_date") \
                         .parquet(output_path)
        
        return final_consumption
    
    def generate_alerts(self, inventory_df: DataFrame) -> DataFrame:
        """
        Generate alerts based on inventory status.
        
        Args:
            inventory_df: Cleaned inventory DataFrame
            
        Returns:
            DataFrame: Generated alerts DataFrame
        """
        from pyspark.sql.functions import datediff, current_date
        
        # Calculate days until expiry
        inventory_with_days = inventory_df.withColumn(
            "days_until_expiry",
            datediff(col("expiry_date"), current_date())
        )
        
        # Generate expiry warnings
        expiry_warnings = inventory_with_days.filter(
            (col("days_until_expiry") <= config.EXPIRY_WARNING_DAYS) &
            (col("days_until_expiry") > 0) &
            (col("status") == "available")
        ).withColumn(
            "alert_id",
            concat(lit("alert_"), col("item_id"), lit("_"), current_timestamp().cast("string"))
        ).withColumn(
            "alert_type", lit("expiry_warning")
        ).withColumn(
            "message",
            concat(lit("Item expires in "), col("days_until_expiry").cast("string"), lit(" days"))
        ).withColumn(
            "created_date", current_timestamp()
        ).withColumn(
            "is_read", lit(False)
        )
        
        # Generate critical expiry alerts
        critical_alerts = inventory_with_days.filter(
            (col("days_until_expiry") <= config.EXPIRY_CRITICAL_DAYS) &
            (col("days_until_expiry") > 0) &
            (col("status") == "available")
        ).withColumn(
            "alert_id",
            concat(lit("alert_crit_"), col("item_id"), lit("_"), current_timestamp().cast("string"))
        ).withColumn(
            "alert_type", lit("expiry_critical")
        ).withColumn(
            "message",
            concat(lit("URGENT: Item expires in "), col("days_until_expiry").cast("string"), lit(" days!"))
        ).withColumn(
            "created_date", current_timestamp()
        ).withColumn(
            "is_read", lit(False)
        )
        
        # Union all alerts
        all_alerts = expiry_warnings.union(critical_alerts)
        
        # Select final columns
        final_alerts = all_alerts.select(
            "alert_id",
            "item_id",
            "alert_type",
            "message",
            "created_date",
            "is_read"
        )
        
        # Save to silver layer
        output_path = os.path.join(config.SILVER_DIR, "alerts")
        final_alerts.write \
                    .mode("append") \
                    .partitionBy("alert_type") \
                    .parquet(output_path)
        
        return final_alerts
    
    def process_all_bronze_data(self):
        """Process all bronze layer data into silver layer."""
        try:
            # Load bronze data
            inventory_bronze_path = os.path.join(config.BRONZE_DIR, "inventory")
            if os.path.exists(inventory_bronze_path):
                inventory_df = self.spark.read.parquet(inventory_bronze_path)
                
                # Process products
                products_df = self.process_products(inventory_df)
                print(f"Processed {products_df.count()} products")
                
                # Process inventory items
                inventory_items_df = self.process_inventory_items(inventory_df, products_df)
                print(f"Processed {inventory_items_df.count()} inventory items")
                
                # Generate alerts
                alerts_df = self.generate_alerts(inventory_items_df)
                print(f"Generated {alerts_df.count()} alerts")
                
                return True
            else:
                print("No bronze inventory data found")
                return False
                
        except Exception as e:
            print(f"Error processing silver data: {e}")
            return False
    
    def close(self):
        """Close Spark session."""
        self.spark.stop()


if __name__ == "__main__":
    processor = SilverDataProcessor()
    success = processor.process_all_bronze_data()
    processor.close()
    
    if success:
        print("Silver layer processing completed successfully")
    else:
        print("Silver layer processing failed")