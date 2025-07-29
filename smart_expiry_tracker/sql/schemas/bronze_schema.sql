-- PostgreSQL schema for Smart Expiry & Consumption Tracker
-- Bronze Layer: Raw data storage

-- Create database (run manually if needed)
-- CREATE DATABASE smart_expiry_tracker;

-- Connect to the database and create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze Layer Tables
-- Raw receipts data
CREATE TABLE IF NOT EXISTS bronze.raw_receipts (
    receipt_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(255),
    purchase_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    raw_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw inventory entries (manual input)
CREATE TABLE IF NOT EXISTS bronze.raw_inventory (
    entry_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),
    brand VARCHAR(255),
    category VARCHAR(100),
    purchase_date TIMESTAMP,
    expiry_date TIMESTAMP,
    quantity DECIMAL(10,2),
    unit VARCHAR(50),
    cost DECIMAL(10,2),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw consumption logs
CREATE TABLE IF NOT EXISTS bronze.raw_consumption (
    log_id VARCHAR(50) PRIMARY KEY,
    item_identifier VARCHAR(255),
    consumption_date TIMESTAMP,
    quantity_consumed DECIMAL(10,2),
    user_id VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_raw_receipts_date ON bronze.raw_receipts(purchase_date);
CREATE INDEX IF NOT EXISTS idx_raw_inventory_date ON bronze.raw_inventory(purchase_date);
CREATE INDEX IF NOT EXISTS idx_raw_consumption_date ON bronze.raw_consumption(consumption_date);