-- Silver Layer: Cleaned and transformed data

-- Products master table
CREATE TABLE IF NOT EXISTS silver.products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    brand VARCHAR(255),
    unit VARCHAR(50) DEFAULT 'piece',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory items table
CREATE TABLE IF NOT EXISTS silver.inventory_items (
    item_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES silver.products(product_id),
    purchase_date TIMESTAMP NOT NULL,
    expiry_date TIMESTAMP NOT NULL,
    quantity DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'available',
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Consumption records table
CREATE TABLE IF NOT EXISTS silver.consumption_records (
    record_id VARCHAR(50) PRIMARY KEY,
    item_id VARCHAR(50) REFERENCES silver.inventory_items(item_id),
    consumption_date TIMESTAMP NOT NULL,
    quantity_consumed DECIMAL(10,2) NOT NULL,
    user_id VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts table
CREATE TABLE IF NOT EXISTS silver.alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    item_id VARCHAR(50) REFERENCES silver.inventory_items(item_id),
    alert_type VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_read BOOLEAN DEFAULT FALSE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_inventory_expiry ON silver.inventory_items(expiry_date);
CREATE INDEX IF NOT EXISTS idx_inventory_status ON silver.inventory_items(status);
CREATE INDEX IF NOT EXISTS idx_consumption_date ON silver.consumption_records(consumption_date);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON silver.alerts(alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_read ON silver.alerts(is_read);

-- Create triggers for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON silver.products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_inventory_updated_at BEFORE UPDATE ON silver.inventory_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();