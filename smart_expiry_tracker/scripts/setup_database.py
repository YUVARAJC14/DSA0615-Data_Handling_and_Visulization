#!/usr/bin/env python3
"""
Database setup script for Smart Expiry & Consumption Tracker.

This script creates the PostgreSQL database schemas and tables.
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add src directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from src.models.config import config


def create_database():
    """Create the database if it doesn't exist."""
    try:
        # Connect to default postgres database to create our database
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
            (config.DB_NAME,)
        )
        
        if not cursor.fetchone():
            cursor.execute(f'CREATE DATABASE "{config.DB_NAME}"')
            print(f"✓ Created database: {config.DB_NAME}")
        else:
            print(f"✓ Database already exists: {config.DB_NAME}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False


def execute_schema_file(file_path, description):
    """Execute SQL schema file."""
    try:
        # Connect to our database
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        cursor = conn.cursor()
        
        # Read and execute SQL file
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        print(f"✓ Executed {description}: {os.path.basename(file_path)}")
        return True
        
    except Exception as e:
        print(f"❌ Error executing {description}: {e}")
        return False


def setup_database():
    """Set up the complete database schema."""
    print("🗄️ Setting up Smart Expiry Tracker database...")
    
    # Create database
    if not create_database():
        return False
    
    # Get schema file paths
    sql_dir = os.path.join(project_root, "sql", "schemas")
    
    schema_files = [
        ("bronze_schema.sql", "Bronze layer schema"),
        ("silver_schema.sql", "Silver layer schema"),
        ("gold_schema.sql", "Gold layer schema")
    ]
    
    # Execute schema files
    for filename, description in schema_files:
        file_path = os.path.join(sql_dir, filename)
        if os.path.exists(file_path):
            if not execute_schema_file(file_path, description):
                return False
        else:
            print(f"⚠ Schema file not found: {filename}")
    
    print("\n✅ Database setup completed successfully!")
    print(f"Database: {config.DB_NAME}")
    print(f"Host: {config.DB_HOST}:{config.DB_PORT}")
    print("Schemas: bronze, silver, gold")
    
    return True


def test_connection():
    """Test database connection."""
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            database=config.DB_NAME
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✓ Database connection successful")
        print(f"PostgreSQL version: {version}")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure PostgreSQL is running")
        print("2. Check database credentials in environment variables")
        print("3. Verify network connectivity to database host")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Database setup for Smart Expiry Tracker")
    parser.add_argument("--test", action="store_true", help="Test database connection only")
    parser.add_argument("--force", action="store_true", help="Force recreate database")
    
    args = parser.parse_args()
    
    if args.test:
        test_connection()
    else:
        print("📊 Smart Expiry & Consumption Tracker - Database Setup")
        print(f"Target database: {config.database_url}")
        print()
        
        if setup_database():
            print("\n🎉 Setup completed! You can now run the demo pipeline.")
        else:
            print("\n💥 Setup failed. Please check the error messages above.")
            sys.exit(1)