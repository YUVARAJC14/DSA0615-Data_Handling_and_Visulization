# Architecture Overview

This document provides a detailed overview of the Smart Expiry & Consumption Tracker architecture.

## Medallion Architecture

The project implements the Medallion Architecture pattern, which provides a systematic approach to building data lakehouses through a multi-hop architecture:

```
Raw Data Sources → Bronze → Silver → Gold → Business Intelligence
```

### Bronze Layer (Raw Data)

**Purpose**: Ingests raw data from various sources with minimal processing.

**Characteristics**:
- Stores data in its original format
- Preserves data lineage and audit trail
- Handles schema evolution gracefully
- Optimized for write performance

**Data Sources**:
- Manual inventory entries
- Receipt text (OCR or manual input)
- Consumption logs
- IoT sensor data (future enhancement)

**Storage Format**: Parquet files partitioned by date/category

### Silver Layer (Cleaned Data)

**Purpose**: Cleans, validates, and standardizes data from the bronze layer.

**Transformations**:
- Data quality checks and validation
- Schema standardization
- Deduplication
- Product normalization
- Category standardization
- Data type conversions

**Key Entities**:
- **Products**: Master data for all products
- **Inventory Items**: Individual items with expiry tracking
- **Consumption Records**: Usage and consumption tracking
- **Alerts**: System-generated notifications

### Gold Layer (Business Insights)

**Purpose**: Creates aggregated, business-ready datasets for analytics and reporting.

**Analytics Outputs**:
- Daily inventory summaries
- Category-wise analytics
- Consumption patterns
- Waste analysis
- Expiry predictions
- User insights and recommendations

## Data Flow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Sources   │    │  Bronze Layer   │    │  Silver Layer   │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Receipts      │───▶│ • raw_receipts  │───▶│ • products      │
│ • Manual Entry  │    │ • raw_inventory │    │ • inventory     │
│ • Consumption   │    │ • raw_consumption│    │ • consumption   │
│ • IoT Sensors   │    │                 │    │ • alerts        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                       ┌─────────────────┐             │
                       │   Gold Layer    │◀────────────┘
                       ├─────────────────┤
                       │ • daily_summary │
                       │ • category_analytics│
                       │ • consumption_patterns│
                       │ • waste_analysis│
                       │ • expiry_predictions│
                       │ • user_insights │
                       └─────────────────┘
                                │
                       ┌─────────────────┐
                       │   Applications  │
                       ├─────────────────┤
                       │ • Dashboard     │
                       │ • Mobile App    │
                       │ • Notifications │
                       │ • Reports       │
                       └─────────────────┘
```

## Technology Stack

### Core Components

1. **PySpark**: 
   - Distributed data processing engine
   - Handles large-scale transformations
   - Provides SQL interface for complex analytics

2. **PostgreSQL**:
   - Relational database for structured storage
   - ACID compliance for critical data
   - Rich query capabilities

3. **Python**:
   - Application logic and data models
   - API development
   - Integration with external services

### Storage Strategy

- **Bronze**: Parquet files with date/category partitioning
- **Silver**: Parquet files with status/type partitioning  
- **Gold**: Parquet files optimized for analytics queries
- **Metadata**: PostgreSQL for schema and configuration

## Scalability Considerations

### Horizontal Scaling
- PySpark can scale across multiple nodes
- Partitioned storage supports parallel processing
- Medallion architecture enables independent layer scaling

### Performance Optimization
- Columnar storage with Parquet
- Partition pruning for efficient queries
- Adaptive query execution in Spark
- Predicate pushdown optimization

### Data Governance
- Schema evolution support
- Data lineage tracking
- Quality metrics and monitoring
- Audit trails in bronze layer

## Security & Privacy

### Data Protection
- Sensitive data masking in non-production environments
- Encryption at rest and in transit
- Access control through role-based permissions

### Compliance
- Data retention policies
- GDPR compliance considerations
- Audit logging for all data operations

## Future Enhancements

### Planned Features
1. **Real-time Processing**: Stream processing for IoT data
2. **ML Integration**: Advanced prediction models
3. **API Layer**: REST/GraphQL APIs for external access
4. **UI/UX**: Web and mobile applications
5. **Integration**: Third-party service connections

### Extensibility Points
- Plugin architecture for new data sources
- Configurable transformation rules
- Custom analytics modules
- External notification systems

## Monitoring & Observability

### Metrics Collection
- Data quality metrics
- Processing performance
- Business KPIs
- System health indicators

### Alerting
- Data pipeline failures
- Quality threshold breaches
- System resource issues
- Business rule violations

This architecture provides a robust foundation for household goods tracking while maintaining flexibility for future growth and feature additions.