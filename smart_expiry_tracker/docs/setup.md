# Setup Guide

This guide will help you set up and run the Smart Expiry & Consumption Tracker project.

## Prerequisites

- Python 3.8 or higher
- Java 8 or 11 (required for PySpark)
- PostgreSQL (optional, for production use)

## Installation

1. **Clone the repository** (if not already done):
   ```bash
   git clone <repository-url>
   cd DSA0615-Data_Handling_and_Visulization/smart_expiry_tracker
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify Java installation**:
   ```bash
   java -version
   ```
   
   If Java is not installed, install OpenJDK:
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install openjdk-11-jdk
   
   # MacOS
   brew install openjdk@11
   
   # Windows
   # Download and install from Oracle or OpenJDK website
   ```

## Configuration

The project uses environment variables for configuration. You can set these in your shell or create a `.env` file:

```bash
# Database configuration (optional for demo)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=smart_expiry_tracker
export DB_USER=postgres
export DB_PASSWORD=postgres

# Spark configuration
export SPARK_MASTER=local[*]  # Use all available cores
```

## Quick Start

1. **Run the complete demo**:
   ```bash
   python demo/demo_pipeline.py
   ```

2. **Run individual layers**:
   ```bash
   # Bronze layer only
   python demo/demo_pipeline.py --bronze
   
   # Silver layer only
   python demo/demo_pipeline.py --silver
   
   # Gold layer only
   python demo/demo_pipeline.py --gold
   
   # Show project info
   python demo/demo_pipeline.py --info
   ```

## Database Setup (Optional)

For production use with PostgreSQL:

1. **Install PostgreSQL**:
   ```bash
   # Ubuntu/Debian
   sudo apt install postgresql postgresql-contrib
   
   # MacOS
   brew install postgresql
   
   # Windows
   # Download from postgresql.org
   ```

2. **Create database**:
   ```sql
   createdb smart_expiry_tracker
   ```

3. **Run schema creation**:
   ```bash
   psql -d smart_expiry_tracker -f sql/schemas/bronze_schema.sql
   psql -d smart_expiry_tracker -f sql/schemas/silver_schema.sql
   psql -d smart_expiry_tracker -f sql/schemas/gold_schema.sql
   ```

## Troubleshooting

### Common Issues

1. **Java not found error**:
   - Install Java JDK 8 or 11
   - Set JAVA_HOME environment variable

2. **PySpark installation issues**:
   - Try installing with specific version: `pip install pyspark==3.5.0`
   - Ensure Java is properly installed

3. **Permission errors**:
   - Ensure you have write permissions to the project directory
   - Check that data directories can be created

4. **Memory issues**:
   - Reduce Spark parallelism: `export SPARK_MASTER=local[2]`
   - Adjust Spark memory settings if needed

### Getting Help

- Check the logs in the console output
- Verify all dependencies are installed
- Ensure sample data files exist in `sample_data/` directory
- Check that the project structure is intact

## Next Steps

After successful setup:

1. Explore the generated data in the `data/` directory
2. Modify sample data to test different scenarios
3. Extend the analytics in the gold layer
4. Implement additional data sources
5. Build a user interface for the insights