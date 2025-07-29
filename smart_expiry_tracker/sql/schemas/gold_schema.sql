-- Gold Layer: Analytics and aggregated insights

-- Daily inventory summary
CREATE TABLE IF NOT EXISTS gold.daily_inventory_summary (
    summary_date DATE PRIMARY KEY,
    total_items INTEGER,
    total_value DECIMAL(12,2),
    expiring_soon_count INTEGER,
    expired_count INTEGER,
    available_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Category analytics
CREATE TABLE IF NOT EXISTS gold.category_analytics (
    category VARCHAR(100),
    month_year VARCHAR(7), -- YYYY-MM format
    total_purchased INTEGER,
    total_consumed INTEGER,
    total_wasted INTEGER,
    waste_percentage DECIMAL(5,2),
    avg_days_to_consume DECIMAL(5,2),
    PRIMARY KEY (category, month_year)
);

-- Consumption patterns
CREATE TABLE IF NOT EXISTS gold.consumption_patterns (
    product_id VARCHAR(50),
    avg_consumption_rate DECIMAL(10,4), -- items per day
    seasonal_factor DECIMAL(5,2),
    predicted_next_purchase DATE,
    confidence_score DECIMAL(3,2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);

-- Waste analysis
CREATE TABLE IF NOT EXISTS gold.waste_analysis (
    analysis_date DATE,
    category VARCHAR(100),
    total_wasted_items INTEGER,
    total_wasted_value DECIMAL(10,2),
    top_wasted_products TEXT[], -- Array of product names
    waste_reasons TEXT[],
    PRIMARY KEY (analysis_date, category)
);

-- Expiry predictions
CREATE TABLE IF NOT EXISTS gold.expiry_predictions (
    item_id VARCHAR(50) PRIMARY KEY,
    predicted_consumption_date DATE,
    confidence_level VARCHAR(20), -- high, medium, low
    recommendation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User insights summary
CREATE TABLE IF NOT EXISTS gold.user_insights (
    insight_id VARCHAR(50) PRIMARY KEY,
    insight_type VARCHAR(100), -- waste_pattern, buying_habit, etc.
    title VARCHAR(255),
    description TEXT,
    action_recommendation TEXT,
    priority_level INTEGER, -- 1-5, 1 being highest
    created_date DATE DEFAULT CURRENT_DATE
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON gold.daily_inventory_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_category_analytics_month ON gold.category_analytics(month_year);
CREATE INDEX IF NOT EXISTS idx_consumption_patterns_rate ON gold.consumption_patterns(avg_consumption_rate);
CREATE INDEX IF NOT EXISTS idx_waste_analysis_date ON gold.waste_analysis(analysis_date);
CREATE INDEX IF NOT EXISTS idx_expiry_predictions_date ON gold.expiry_predictions(predicted_consumption_date);
CREATE INDEX IF NOT EXISTS idx_user_insights_priority ON gold.user_insights(priority_level);