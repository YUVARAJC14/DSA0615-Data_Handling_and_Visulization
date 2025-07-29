# Smart Expiry & Consumption Tracker

A data engineering project that helps users track the expiry, freshness, and consumption rates of their household goods using Medallion Architecture, PySpark, and PostgreSQL.

## Project Overview

This project implements a comprehensive tracking system for household goods to reduce waste and optimize consumption patterns. It leverages modern data engineering practices with a Medallion Architecture approach.

## Architecture

The project follows the Medallion Architecture with three distinct layers:

- **Bronze Layer**: Raw data ingestion from receipts, manual entries, and inventory inputs
- **Silver Layer**: Data cleaning, parsing, and transformation of product information
- **Gold Layer**: Analytics, consumption patterns, waste analysis, and smart recommendations

## Technology Stack

- **PySpark**: Data processing and transformation
- **PostgreSQL**: Data storage and persistence
- **Python**: Core application logic and data models
- **Medallion Architecture**: Data lakehouse pattern implementation

## Features

- 📱 Manual inventory entry and receipt processing
- 📅 Expiry date tracking and alerts
- 📊 Consumption pattern analysis
- 🗑️ Waste reduction recommendations
- 📈 Purchase pattern insights
- 🔔 Smart notifications for upcoming expiries

## Quick Start

1. **Setup Environment**
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize Database**
   ```bash
   python scripts/setup_database.py
   ```

3. **Run Demo**
   ```bash
   python demo/demo_pipeline.py
   ```

## Project Structure

```
smart_expiry_tracker/
├── README.md
├── requirements.txt
├── data/
│   ├── bronze/          # Raw data layer
│   ├── silver/          # Cleaned data layer
│   └── gold/            # Analytics layer
├── src/
│   ├── models/          # Data models
│   ├── bronze/          # Bronze layer processing
│   ├── silver/          # Silver layer processing
│   └── gold/            # Gold layer processing
├── sql/
│   └── schemas/         # PostgreSQL schemas
├── sample_data/         # Sample data for testing
├── demo/                # Demo scripts
└── docs/                # Additional documentation
```

## Data Model

### Core Entities

1. **Products**: Basic product information (name, category, brand)
2. **Inventory**: Items with purchase/expiry dates and quantities
3. **Consumption**: Usage tracking and consumption patterns
4. **Alerts**: Expiry notifications and recommendations

### Data Flow

1. **Bronze**: Raw data ingestion from various sources
2. **Silver**: Data cleaning, validation, and standardization
3. **Gold**: Aggregated analytics and business insights

## Getting Started

See the detailed documentation in the `docs/` directory for:
- [Setup Guide](docs/setup.md)
- [Architecture Overview](docs/architecture.md)
- [API Documentation](docs/api.md)
- [Development Guide](docs/development.md)

## Contributing

This project is part of the DSA0615 Data Handling and Visualization course. For contributions and improvements, please follow the project guidelines.

## License

This project is for educational purposes as part of the DSA0615 course.