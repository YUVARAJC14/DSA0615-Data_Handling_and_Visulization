"""
Gold layer analytics processing for Smart Expiry & Consumption Tracker.

This module handles advanced analytics, insights generation, and predictions.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, current_date, datediff, date_format, round as spark_round,
    collect_list, size, array_contains, current_timestamp, lit,
    window, desc, asc, row_number, rank, dense_rank, array
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.models.config import config


class GoldAnalyticsProcessor:
    """Handle analytics and insights generation for gold layer."""
    
    def __init__(self):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(config.SPARK_APP_NAME + "_Gold") \
            .master(config.SPARK_MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Ensure gold directory exists
        os.makedirs(config.GOLD_DIR, exist_ok=True)
    
    def load_silver_data(self):
        """Load data from silver layer."""
        try:
            # Load silver layer data
            products_path = os.path.join(config.SILVER_DIR, "products")
            inventory_path = os.path.join(config.SILVER_DIR, "inventory_items")
            consumption_path = os.path.join(config.SILVER_DIR, "consumption_records")
            
            self.products_df = None
            self.inventory_df = None
            self.consumption_df = None
            
            if os.path.exists(products_path):
                self.products_df = self.spark.read.parquet(products_path)
            
            if os.path.exists(inventory_path):
                self.inventory_df = self.spark.read.parquet(inventory_path)
                
            if os.path.exists(consumption_path):
                self.consumption_df = self.spark.read.parquet(consumption_path)
                
            return True
        except Exception as e:
            print(f"Error loading silver data: {e}")
            return False
    
    def generate_daily_inventory_summary(self) -> DataFrame:
        """
        Generate daily inventory summary analytics.
        
        Returns:
            DataFrame: Daily inventory summary
        """
        if self.inventory_df is None:
            return None
        
        # Join with products for enriched data
        enriched_inventory = self.inventory_df.join(
            self.products_df,
            "product_id",
            "left"
        ) if self.products_df is not None else self.inventory_df
        
        # Calculate days until expiry
        inventory_with_days = enriched_inventory.withColumn(
            "days_until_expiry",
            datediff(col("expiry_date"), current_date())
        )
        
        # Generate summary for current date
        summary = inventory_with_days.agg(
            count("*").alias("total_items"),
            spark_sum(col("cost")).alias("total_value"),
            spark_sum(when(
                (col("days_until_expiry") <= 7) & (col("days_until_expiry") > 0) & (col("status") == "available"), 1
            ).otherwise(0)).alias("expiring_soon_count"),
            spark_sum(when(
                (col("days_until_expiry") <= 0) | (col("status") == "expired"), 1
            ).otherwise(0)).alias("expired_count"),
            spark_sum(when(col("status") == "available", 1).otherwise(0)).alias("available_count")
        ).withColumn("summary_date", current_date()) \
         .withColumn("created_at", current_timestamp())
        
        # Reorder columns
        daily_summary = summary.select(
            "summary_date", "total_items", "total_value", 
            "expiring_soon_count", "expired_count", "available_count", "created_at"
        )
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "daily_inventory_summary")
        daily_summary.write \
                     .mode("append") \
                     .parquet(output_path)
        
        return daily_summary
    
    def generate_category_analytics(self) -> DataFrame:
        """
        Generate category-wise analytics.
        
        Returns:
            DataFrame: Category analytics
        """
        if self.inventory_df is None or self.products_df is None:
            return None
        
        # Join inventory with products
        inventory_products = self.inventory_df.join(self.products_df, "product_id", "inner")
        
        # Add month_year column
        inventory_with_month = inventory_products.withColumn(
            "month_year",
            date_format(col("purchase_date"), "yyyy-MM")
        )
        
        # Calculate consumption days (simplified)
        inventory_with_consumption = inventory_with_month.withColumn(
            "days_to_consume",
            when(col("status") == "consumed", 
                 datediff(current_date(), col("purchase_date"))
            ).otherwise(None)
        )
        
        # Group by category and month
        category_analytics = inventory_with_consumption.groupBy("category", "month_year").agg(
            count("*").alias("total_purchased"),
            spark_sum(when(col("status") == "consumed", 1).otherwise(0)).alias("total_consumed"),
            spark_sum(when(col("status").isin(["expired", "wasted"]), 1).otherwise(0)).alias("total_wasted"),
            avg("days_to_consume").alias("avg_days_to_consume")
        )
        
        # Calculate waste percentage
        category_analytics = category_analytics.withColumn(
            "waste_percentage",
            spark_round((col("total_wasted") / col("total_purchased")) * 100, 2)
        )
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "category_analytics")
        category_analytics.write \
                         .mode("overwrite") \
                         .partitionBy("month_year") \
                         .parquet(output_path)
        
        return category_analytics
    
    def generate_consumption_patterns(self) -> DataFrame:
        """
        Generate consumption patterns and predictions.
        
        Returns:
            DataFrame: Consumption patterns
        """
        if self.inventory_df is None or self.consumption_df is None:
            return None
        
        # Join inventory with consumption
        consumption_inventory = self.consumption_df.join(
            self.inventory_df, "item_id", "inner"
        )
        
        # Group by product to calculate patterns
        consumption_patterns = consumption_inventory.groupBy("product_id").agg(
            avg("quantity_consumed").alias("avg_consumption_rate"),
            count("*").alias("consumption_frequency"),
            spark_max("consumption_date").alias("last_consumption_date")
        )
        
        # Calculate seasonal factor (simplified)
        consumption_patterns = consumption_patterns.withColumn(
            "seasonal_factor", lit(1.0)  # Placeholder for seasonal analysis
        )
        
        # Predict next purchase (simplified logic)
        consumption_patterns = consumption_patterns.withColumn(
            "predicted_next_purchase",
            col("last_consumption_date") + 30  # Assume 30-day cycle
        ).withColumn(
            "confidence_score", 
            when(col("consumption_frequency") >= 5, 0.8)
            .when(col("consumption_frequency") >= 3, 0.6)
            .otherwise(0.4)
        ).withColumn(
            "last_updated", current_timestamp()
        )
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "consumption_patterns")
        consumption_patterns.write \
                           .mode("overwrite") \
                           .parquet(output_path)
        
        return consumption_patterns
    
    def generate_waste_analysis(self) -> DataFrame:
        """
        Generate waste analysis insights.
        
        Returns:
            DataFrame: Waste analysis
        """
        if self.inventory_df is None or self.products_df is None:
            return None
        
        # Join inventory with products
        inventory_products = self.inventory_df.join(self.products_df, "product_id", "inner")
        
        # Filter wasted items
        wasted_items = inventory_products.filter(
            col("status").isin(["expired", "wasted"])
        )
        
        # Add analysis date
        wasted_with_date = wasted_items.withColumn(
            "analysis_date", current_date()
        )
        
        # Group by category and analysis date
        waste_analysis = wasted_with_date.groupBy("analysis_date", "category").agg(
            count("*").alias("total_wasted_items"),
            spark_sum("cost").alias("total_wasted_value"),
            collect_list("name").alias("wasted_products_list")
        )
        
        # Convert product list to top wasted products (simplified)
        waste_analysis = waste_analysis.withColumn(
            "top_wasted_products",
            col("wasted_products_list")  # In real scenario, would aggregate and rank
        ).withColumn(
            "waste_reasons",
            array(lit("expired"), lit("forgotten"), lit("over_purchase"))  # Simplified reasons
        )
        
        # Select final columns
        waste_analysis = waste_analysis.select(
            "analysis_date", "category", "total_wasted_items", 
            "total_wasted_value", "top_wasted_products", "waste_reasons"
        )
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "waste_analysis")
        waste_analysis.write \
                     .mode("append") \
                     .partitionBy("analysis_date") \
                     .parquet(output_path)
        
        return waste_analysis
    
    def generate_expiry_predictions(self) -> DataFrame:
        """
        Generate expiry predictions and recommendations.
        
        Returns:
            DataFrame: Expiry predictions
        """
        if self.inventory_df is None:
            return None
        
        # Calculate days until expiry
        inventory_with_days = self.inventory_df.withColumn(
            "days_until_expiry",
            datediff(col("expiry_date"), current_date())
        ).filter(
            (col("status") == "available") & (col("days_until_expiry") > 0)
        )
        
        # Generate predictions based on expiry timeline
        expiry_predictions = inventory_with_days.withColumn(
            "predicted_consumption_date",
            when(col("days_until_expiry") <= 3, current_date() + 1)  # Urgent: consume tomorrow
            .when(col("days_until_expiry") <= 7, current_date() + 3)  # Soon: consume in 3 days
            .otherwise(current_date() + 7)  # Normal: consume within a week
        ).withColumn(
            "confidence_level",
            when(col("days_until_expiry") <= 3, "high")
            .when(col("days_until_expiry") <= 7, "medium")
            .otherwise("low")
        ).withColumn(
            "recommendation",
            when(col("days_until_expiry") <= 1, "URGENT: Use immediately!")
            .when(col("days_until_expiry") <= 3, "Use within next 2 days")
            .when(col("days_until_expiry") <= 7, "Plan to use this week")
            .otherwise("Monitor expiry date")
        ).withColumn(
            "created_at", current_timestamp()
        )
        
        # Select final columns
        final_predictions = expiry_predictions.select(
            "item_id", "predicted_consumption_date", 
            "confidence_level", "recommendation", "created_at"
        )
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "expiry_predictions")
        final_predictions.write \
                         .mode("overwrite") \
                         .parquet(output_path)
        
        return final_predictions
    
    def generate_user_insights(self) -> DataFrame:
        """
        Generate high-level user insights and recommendations.
        
        Returns:
            DataFrame: User insights
        """
        insights_data = []
        
        if self.inventory_df is not None:
            # Calculate basic statistics
            total_items = self.inventory_df.count()
            expired_items = self.inventory_df.filter(col("status") == "expired").count()
            
            if total_items > 0:
                waste_rate = (expired_items / total_items) * 100
                
                # Generate insights based on data
                if waste_rate > 20:
                    insights_data.append({
                        "insight_id": "waste_high",
                        "insight_type": "waste_pattern",
                        "title": "High Waste Rate Detected",
                        "description": f"You're wasting {waste_rate:.1f}% of your items",
                        "action_recommendation": "Focus on using items before expiry and reduce purchase quantities",
                        "priority_level": 1,
                        "created_date": datetime.now().date()
                    })
                
                if total_items > 50:
                    insights_data.append({
                        "insight_id": "inventory_large",
                        "insight_type": "buying_habit",
                        "title": "Large Inventory Detected",
                        "description": f"You have {total_items} items in inventory",
                        "action_recommendation": "Consider reducing purchase frequency to avoid spoilage",
                        "priority_level": 2,
                        "created_date": datetime.now().date()
                    })
        
        # Create default insights if no data available
        if not insights_data:
            insights_data.append({
                "insight_id": "welcome",
                "insight_type": "general",
                "title": "Welcome to Smart Expiry Tracker",
                "description": "Start by adding items to your inventory to get personalized insights",
                "action_recommendation": "Add some household items to begin tracking",
                "priority_level": 3,
                "created_date": datetime.now().date()
            })
        
        # Create DataFrame
        insights_df = self.spark.createDataFrame(insights_data)
        
        # Save to gold layer
        output_path = os.path.join(config.GOLD_DIR, "user_insights")
        insights_df.write \
                   .mode("overwrite") \
                   .parquet(output_path)
        
        return insights_df
    
    def process_all_analytics(self):
        """Process all gold layer analytics."""
        try:
            if not self.load_silver_data():
                print("Failed to load silver data")
                return False
            
            print("Generating analytics...")
            
            # Generate daily summary
            daily_summary = self.generate_daily_inventory_summary()
            if daily_summary:
                print(f"Generated daily inventory summary: {daily_summary.count()} records")
            
            # Generate category analytics
            category_analytics = self.generate_category_analytics()
            if category_analytics:
                print(f"Generated category analytics: {category_analytics.count()} records")
            
            # Generate consumption patterns
            consumption_patterns = self.generate_consumption_patterns()
            if consumption_patterns:
                print(f"Generated consumption patterns: {consumption_patterns.count()} records")
            
            # Generate waste analysis
            waste_analysis = self.generate_waste_analysis()
            if waste_analysis:
                print(f"Generated waste analysis: {waste_analysis.count()} records")
            
            # Generate expiry predictions
            expiry_predictions = self.generate_expiry_predictions()
            if expiry_predictions:
                print(f"Generated expiry predictions: {expiry_predictions.count()} records")
            
            # Generate user insights
            user_insights = self.generate_user_insights()
            if user_insights:
                print(f"Generated user insights: {user_insights.count()} records")
            
            return True
            
        except Exception as e:
            print(f"Error processing gold analytics: {e}")
            return False
    
    def close(self):
        """Close Spark session."""
        self.spark.stop()


if __name__ == "__main__":
    processor = GoldAnalyticsProcessor()
    success = processor.process_all_analytics()
    processor.close()
    
    if success:
        print("Gold layer analytics processing completed successfully")
    else:
        print("Gold layer analytics processing failed")