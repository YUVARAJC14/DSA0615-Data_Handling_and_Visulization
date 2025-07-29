#!/usr/bin/env python3
"""
Demo script for Smart Expiry & Consumption Tracker.

This script demonstrates the complete data pipeline from bronze to gold layers.
"""

import os
import sys
import json
from datetime import datetime

# Add src directory to path
demo_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(demo_dir)
sys.path.append(project_root)

from src.bronze.data_ingestion import BronzeDataIngestion
from src.silver.data_transformation import SilverDataProcessor
from src.gold.analytics_processor import GoldAnalyticsProcessor
from src.models.config import config


def print_banner(text):
    """Print a formatted banner."""
    print("\n" + "="*60)
    print(f" {text}")
    print("="*60)


def print_step(step_num, description):
    """Print a step description."""
    print(f"\n{step_num}. {description}")
    print("-" * 40)


def demo_bronze_layer():
    """Demonstrate bronze layer data ingestion."""
    print_banner("BRONZE LAYER - Raw Data Ingestion")
    
    ingestion = BronzeDataIngestion()
    
    try:
        # Load sample data files
        sample_data_dir = os.path.join(project_root, "sample_data")
        
        print_step(1, "Loading sample inventory data")
        inventory_file = os.path.join(sample_data_dir, "sample_inventory.json")
        if os.path.exists(inventory_file):
            inventory_df = ingestion.load_from_json(inventory_file, "inventory")
            print(f"✓ Ingested {inventory_df.count()} inventory records")
            inventory_df.show(5, truncate=False)
        else:
            print("⚠ Sample inventory file not found")
        
        print_step(2, "Loading sample consumption data")
        consumption_file = os.path.join(sample_data_dir, "sample_consumption.json")
        if os.path.exists(consumption_file):
            consumption_df = ingestion.load_from_json(consumption_file, "consumption")
            print(f"✓ Ingested {consumption_df.count()} consumption records")
            consumption_df.show(5, truncate=False)
        else:
            print("⚠ Sample consumption file not found")
        
        print_step(3, "Loading sample receipt data")
        receipts_file = os.path.join(sample_data_dir, "sample_receipts.json")
        if os.path.exists(receipts_file):
            receipts_df = ingestion.load_from_json(receipts_file, "receipts")
            print(f"✓ Ingested {receipts_df.count()} receipt records")
            receipts_df.show(3, truncate=False)
        else:
            print("⚠ Sample receipts file not found")
        
        print("\n✅ Bronze layer ingestion completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Bronze layer error: {e}")
        return False
    finally:
        ingestion.close()


def demo_silver_layer():
    """Demonstrate silver layer data processing."""
    print_banner("SILVER LAYER - Data Cleaning & Transformation")
    
    processor = SilverDataProcessor()
    
    try:
        print_step(1, "Processing bronze data into silver layer")
        success = processor.process_all_bronze_data()
        
        if success:
            print("\n✅ Silver layer processing completed successfully!")
            
            # Show sample results
            print_step(2, "Sample silver layer data")
            
            # Load and display products
            products_path = os.path.join(config.SILVER_DIR, "products")
            if os.path.exists(products_path):
                products_df = processor.spark.read.parquet(products_path)
                print(f"📦 Products ({products_df.count()} records):")
                products_df.show(5, truncate=False)
            
            # Load and display inventory items
            inventory_path = os.path.join(config.SILVER_DIR, "inventory_items")
            if os.path.exists(inventory_path):
                inventory_df = processor.spark.read.parquet(inventory_path)
                print(f"📋 Inventory Items ({inventory_df.count()} records):")
                inventory_df.show(5, truncate=False)
            
            # Load and display alerts
            alerts_path = os.path.join(config.SILVER_DIR, "alerts")
            if os.path.exists(alerts_path):
                alerts_df = processor.spark.read.parquet(alerts_path)
                print(f"🚨 Alerts ({alerts_df.count()} records):")
                alerts_df.show(5, truncate=False)
            
            return True
        else:
            print("❌ Silver layer processing failed")
            return False
            
    except Exception as e:
        print(f"❌ Silver layer error: {e}")
        return False
    finally:
        processor.close()


def demo_gold_layer():
    """Demonstrate gold layer analytics processing."""
    print_banner("GOLD LAYER - Analytics & Insights")
    
    processor = GoldAnalyticsProcessor()
    
    try:
        print_step(1, "Generating analytics and insights")
        success = processor.process_all_analytics()
        
        if success:
            print("\n✅ Gold layer analytics completed successfully!")
            
            # Show sample analytics results
            print_step(2, "Sample analytics results")
            
            # Daily inventory summary
            summary_path = os.path.join(config.GOLD_DIR, "daily_inventory_summary")
            if os.path.exists(summary_path):
                summary_df = processor.spark.read.parquet(summary_path)
                print(f"📊 Daily Inventory Summary ({summary_df.count()} records):")
                summary_df.show(truncate=False)
            
            # Category analytics
            category_path = os.path.join(config.GOLD_DIR, "category_analytics")
            if os.path.exists(category_path):
                category_df = processor.spark.read.parquet(category_path)
                print(f"📈 Category Analytics ({category_df.count()} records):")
                category_df.show(truncate=False)
            
            # Expiry predictions
            predictions_path = os.path.join(config.GOLD_DIR, "expiry_predictions")
            if os.path.exists(predictions_path):
                predictions_df = processor.spark.read.parquet(predictions_path)
                print(f"🔮 Expiry Predictions ({predictions_df.count()} records):")
                predictions_df.show(5, truncate=False)
            
            # User insights
            insights_path = os.path.join(config.GOLD_DIR, "user_insights")
            if os.path.exists(insights_path):
                insights_df = processor.spark.read.parquet(insights_path)
                print(f"💡 User Insights ({insights_df.count()} records):")
                insights_df.show(truncate=False)
            
            return True
        else:
            print("❌ Gold layer analytics failed")
            return False
            
    except Exception as e:
        print(f"❌ Gold layer error: {e}")
        return False
    finally:
        processor.close()


def demo_end_to_end_pipeline():
    """Run the complete end-to-end pipeline demo."""
    print_banner("SMART EXPIRY & CONSUMPTION TRACKER - COMPLETE DEMO")
    
    print("🏠 Welcome to the Smart Expiry & Consumption Tracker!")
    print("This demo showcases the complete Medallion Architecture pipeline.")
    print(f"📁 Data will be stored in: {config.DATA_DIR}")
    
    # Ensure data directories exist
    os.makedirs(config.BRONZE_DIR, exist_ok=True)
    os.makedirs(config.SILVER_DIR, exist_ok=True)
    os.makedirs(config.GOLD_DIR, exist_ok=True)
    
    # Run pipeline stages
    stages = [
        ("Bronze Layer (Raw Data Ingestion)", demo_bronze_layer),
        ("Silver Layer (Data Transformation)", demo_silver_layer),
        ("Gold Layer (Analytics & Insights)", demo_gold_layer)
    ]
    
    results = []
    for stage_name, stage_func in stages:
        print(f"\n🚀 Starting {stage_name}...")
        try:
            result = stage_func()
            results.append((stage_name, result))
        except Exception as e:
            print(f"❌ Error in {stage_name}: {e}")
            results.append((stage_name, False))
    
    # Summary
    print_banner("PIPELINE EXECUTION SUMMARY")
    
    for stage_name, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{stage_name}: {status}")
    
    overall_success = all(result for _, result in results)
    
    if overall_success:
        print("\n🎉 Complete pipeline executed successfully!")
        print("\n📋 What was accomplished:")
        print("  • Raw data ingested into Bronze layer")
        print("  • Data cleaned and transformed in Silver layer") 
        print("  • Analytics and insights generated in Gold layer")
        print("  • Expiry predictions and recommendations created")
        print("  • Waste analysis and consumption patterns identified")
        
        print(f"\n📂 Check the data directory: {config.DATA_DIR}")
        print("   Bronze/: Raw parquet files")
        print("   Silver/: Cleaned and structured data")
        print("   Gold/: Analytics and insights")
        
    else:
        print("\n⚠ Pipeline completed with some failures.")
        print("Check the error messages above for details.")
    
    return overall_success


def show_project_info():
    """Display project information."""
    print_banner("PROJECT INFORMATION")
    
    print("📊 Smart Expiry & Consumption Tracker")
    print("    A data engineering project for household goods management")
    print()
    print("🏗️ Architecture: Medallion (Bronze → Silver → Gold)")
    print("⚙️ Technologies: PySpark, PostgreSQL, Python")
    print("📦 Features:")
    print("   • Inventory tracking and expiry monitoring")
    print("   • Consumption pattern analysis")
    print("   • Waste reduction recommendations")
    print("   • Smart alerts and predictions")
    print()
    print(f"🗂️ Configuration:")
    print(f"   • Data Directory: {config.DATA_DIR}")
    print(f"   • Spark Master: {config.SPARK_MASTER}")
    print(f"   • App Name: {config.SPARK_APP_NAME}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Smart Expiry Tracker Demo")
    parser.add_argument("--info", action="store_true", help="Show project information")
    parser.add_argument("--bronze", action="store_true", help="Run bronze layer demo only")
    parser.add_argument("--silver", action="store_true", help="Run silver layer demo only") 
    parser.add_argument("--gold", action="store_true", help="Run gold layer demo only")
    
    args = parser.parse_args()
    
    if args.info:
        show_project_info()
    elif args.bronze:
        demo_bronze_layer()
    elif args.silver:
        demo_silver_layer()
    elif args.gold:
        demo_gold_layer()
    else:
        # Run complete pipeline
        demo_end_to_end_pipeline()