# SQL Data Warehouse Analytics Project

A **production-grade PostgreSQL data warehouse** implementing the **Medallion Architecture** (Bronze → Silver → Gold) with comprehensive logging, data lineage tracking, and enterprise-grade ETL capabilities.

## 🎯 Project Overview

This project demonstrates a complete data warehouse solution built on PostgreSQL with a focus on:

- **Medallion Architecture**: Bronze (raw) → Silver (cleansed) → Gold (analytics) data layers
- **Centralized Configuration**: Environment-based configuration management via `.env`
- **Comprehensive Logging**: Process tracking, error handling, performance monitoring, and data lineage
- **Modular SQL Generation**: Reusable SQL builders and templates
- **Type Safety**: SQLAlchemy ORM for database operations
- **Enterprise Patterns**: Audit trails, data quality checks, and impact analysis

## 📋 Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Core Components](#core-components)
- [Setup Components](#setup-components)
- [SQL Utilities](#sql-utilities)
- [Logging Infrastructure](#logging-infrastructure)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Development](#development)
- [License](#license)

## 🏗️ Architecture

### Medallion Architecture Layers

```
┌─────────────────┐
│   Data Sources  │ (CRM, ERP Systems)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │ Raw, unprocessed data
│  (bronze.*)     │ - Exact copy of source
└────────┬────────┘ - Append-only
         │          - Full history
         ▼
┌─────────────────┐
│  Silver Layer   │ Cleansed, conformed data
│  (silver.*)     │ - Data validation
└────────┬────────┘ - Deduplication
         │          - Standardization
         ▼
┌─────────────────┐
│   Gold Layer    │ Business-ready analytics
│   (gold.*)      │ - Aggregations
└─────────────────┘ - KPIs & metrics
                    - Optimized for BI tools

┌─────────────────┐
│   Logs Layer    │ Audit & monitoring
│   (logs.*)      │ - Process tracking
└─────────────────┘ - Error logging
                    - Performance metrics
                    - Data lineage
```

### Technology Stack

- **Database**: PostgreSQL 13+
- **ORM**: SQLAlchemy 2.0+
- **Language**: Python 3.8+
- **Configuration**: python-dotenv
- **Monitoring**: psutil (system metrics)

## 🚀 Quick Start

### Prerequisites

```bash
# PostgreSQL 13 or higher
psql --version

# Python 3.8 or higher
python --version
```

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd SQL-Data-Warehouse-Analytics-Project
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment**
```bash
# Copy and edit .env file
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

4. **Run setup**
```bash
# Complete warehouse setup
python -m setup.setup_orchestrator

# Or setup without sample tables
python -m setup.setup_orchestrator --no-samples
```

### Verification

```python
from setup import SetupOrchestrator

orchestrator = SetupOrchestrator()
results = orchestrator.run_complete_setup()

if all(results.values()):
    print("✅ Setup completed successfully!")
```

## 📁 Project Structure

```
SQL-Data-Warehouse-Analytics-Project/
├── core/                          # Core infrastructure
│   ├── __init__.py               # Package exports
│   ├── config.py                 # Configuration management
│   └── logger.py                 # Centralized logging
│
├── datasets/                      # Data storage
│   ├── source_crm/               # CRM source data
│   └── source_erp/               # ERP source data
│
├── logs/                          # Logging infrastructure
│   ├── __init__.py               # Package exports
│   ├── audit_logger.py           # Process & config logging
│   ├── data_lineage.py           # Lineage tracking & analysis
│   ├── error_handler.py          # Error logging & recovery
│   └── performance_monitor.py    # Performance metrics
│
├── medallion/                     # Data layers
│   ├── bronze/                   # Raw data layer
│   ├── silver/                   # Cleansed data layer
│   └── gold/                     # Analytics layer
│
├── setup/                         # Database setup
│   ├── __init__.py               # Package exports
│   ├── create_database.py        # Database creation
│   ├── create_schemas.py         # Schema creation
│   ├── create_logs.py            # Logging tables
│   └── setup_orchestrator.py     # Setup coordination
│
├── sql/                           # SQL utilities
│   ├── __init__.py               # Package exports
│   ├── ddl.py                    # Data Definition Language
│   ├── dml.py                    # Data Manipulation Language
│   ├── query_builder.py          # Query builders
│   └── common_queries.py         # Common patterns
│
├── tests/                         # Test suite
│
├── utils/                         # Utility functions
│   ├── __init__.py               # Package exports
│   └── database_utils.py         # Database utilities
│
├── .env                          # Environment configuration (not in git)
├── .gitignore                    # Git ignore rules
├── LICENSE                       # MIT License
├── README.md                     # This file
└── requirements.txt              # Python dependencies
```

## 🔧 Core Components

### `core/config.py`
**Configuration Management**

Centralized configuration system loading from `.env` file:

```python
from core.config import config

# Database connection
conn_string = config.get_connection_string(use_warehouse=True)
print(f"Connecting to {config.db_host}:{config.db_port}")

# Project paths
data_dir = config.project.data_dir
logs_dir = config.project.logs_dir
```

**Key Features**:
- Single source of truth for all settings
- Type-safe configuration classes
- Environment variable validation
- Connection string generation

### `core/logger.py`
**Centralized Logging**

Unified logging configuration for the entire application:

```python
from core.logger import get_logger

logger = get_logger(__name__)
logger.info("Processing started")
logger.error("An error occurred", exc_info=True)
```

**Key Features**:
- Consistent log formatting
- Module-specific loggers
- File and console output
- Exception tracking

## 🔨 Setup Components

### `setup/create_database.py`
**Database Creation Module**

Creates the target PostgreSQL database with proper encoding and collation:

```python
from setup import DatabaseCreator

creator = DatabaseCreator(
    host='localhost',
    user='postgres',
    password='password',
    admin_db='postgres',
    target_db='warehouse'
)

if creator.create_database():
    print("Database created successfully")
```

**Key Features**:
- UTF-8 encoding with en_GB collation
- Connection termination for cleanup
- Database existence checking
- Uses SQL from `sql.ddl` module

**Integration**:
- Called by [`setup_orchestrator`](#setupsetup_orchestratorpy)
- Uses [`sql.ddl`](#sqlddlpy) for SQL generation
- Connects to admin database (not target)

### `setup/create_schemas.py`
**Schema Creation Module**

Creates all medallion architecture schemas:

```python
from setup import SchemaCreator

creator = SchemaCreator(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

results = creator.create_all_schemas()
# Creates: bronze, silver, gold, logs
```

**Key Features**:
- Four core schemas: bronze, silver, gold, logs
- SQLAlchemy-based operations
- Schema verification and inspection
- Idempotent execution

**Integration**:
- Called by [`setup_orchestrator`](#setupsetup_orchestratorpy)
- Uses [`sql.query_builder`](#sqlquery_builderpy) for metadata queries
- Creates foundation for [`create_logs`](#setupcreate_logspy)

### `setup/create_logs.py`
**Logging Infrastructure Module**

Creates comprehensive logging tables using SQLAlchemy ORM:

```python
from setup import LoggingInfrastructure

logs = LoggingInfrastructure(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

results = logs.create_all_tables()
```

**Tables Created**:
- `logs.process_log` - ETL process execution tracking
- `logs.error_log` - Centralized error logging
- `logs.data_lineage` - Data flow tracking
- `logs.performance_metrics` - Performance monitoring
- `logs.configuration_log` - Configuration changes

**Key Features**:
- SQLAlchemy ORM models
- Foreign key relationships
- JSONB for flexible metadata
- PostgreSQL-specific types

**Integration**:
- Models used by [`logs/`](#logging-infrastructure) package
- Called by [`setup_orchestrator`](#setupsetup_orchestratorpy)
- Foundation for audit and monitoring

### `setup/setup_orchestrator.py`
**Setup Coordination Module**

Orchestrates the complete warehouse setup process:

```python
from setup import SetupOrchestrator

orchestrator = SetupOrchestrator()

# Complete setup
results = orchestrator.run_complete_setup(include_samples=True)

# Individual steps
orchestrator.create_database()
orchestrator.create_schemas()
orchestrator.create_logging_infrastructure()
orchestrator.create_sample_medallion_tables()

# Rollback
orchestrator.rollback_setup(keep_database=False)
```

**Setup Sequence**:
1. Create target database
2. Create medallion schemas (bronze/silver/gold/logs)
3. Create logging infrastructure
4. Create sample tables (optional)

**Key Features**:
- Dependency management
- Process tracking via audit logs
- Comprehensive error handling
- Rollback capabilities
- Step timing and metrics

**Integration**:
- Uses [`DatabaseCreator`](#setupcreate_databasepy)
- Uses [`SchemaCreator`](#setupcreate_schemaspy)
- Uses [`LoggingInfrastructure`](#setupcreate_logspy)
- Uses [`sql.ddl`](#sqlddlpy) for sample tables
- Tracks execution via [`logs.audit_logger`](#logsaudit_loggerpy)

**CLI Usage**:
```bash
# Complete setup
python -m setup.setup_orchestrator

# Setup without samples
python -m setup.setup_orchestrator --no-samples

# Rollback keeping database
python -m setup.setup_orchestrator --rollback --keep-db

# Verbose output
python -m setup.setup_orchestrator --verbose
```

## 📊 SQL Utilities

### `sql/ddl.py`
**Data Definition Language Utilities**

Functions for generating PostgreSQL DDL statements:

```python
from sql.ddl import create_table, create_medallion_table_template

# Standard table
table_sql = create_table(
    schema='bronze',
    table='customer_data',
    columns=[
        {'name': 'id', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY']},
        {'name': 'name', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']}
    ],
    medallion_metadata=True,
    comment='Raw customer data'
)

# Full medallion template
medallion_sql = create_medallion_table_template(
    schema='silver',
    table='customers',
    business_columns=[
        {'name': 'customer_id', 'type': 'INTEGER'},
        {'name': 'customer_name', 'type': 'VARCHAR(255)'}
    ],
    partition_by='created_at'
)
```

**Key Functions**:
- `create_schema()` - Schema creation
- `create_table()` - Table with medallion metadata
- `create_index()` - Performance indexes
- `create_constraint()` - Integrity constraints
- `create_medallion_table_template()` - Full medallion table
- `create_database_sql()` - Database creation
- `drop_database_sql()` - Database cleanup
- `terminate_connections_sql()` - Connection management

**Medallion Metadata Columns**:
- `created_at` - Record creation timestamp
- `updated_at` - Last update timestamp
- `created_by` - User/system that created record
- `updated_by` - User/system that updated record
- `source_system` - Source system identifier
- `batch_id` - ETL batch identifier
- `is_deleted` - Soft delete flag
- `row_hash` - Data integrity hash

**Integration**:
- Used by [`setup_orchestrator`](#setupsetup_orchestratorpy)
- Used by [`create_logs`](#setupcreate_logspy)
- Foundation for all table creation

### `sql/dml.py`
**Data Manipulation Language Utilities**

Functions for generating PostgreSQL DML statements:

```python
from sql.dml import bulk_insert, upsert, merge_statement

# Bulk insert
insert_sql = bulk_insert(
    schema='bronze',
    table='customer_data',
    columns=['customer_id', 'name', 'email'],
    on_conflict='DO NOTHING'
)

# Upsert operation
upsert_sql = upsert(
    schema='silver',
    table='customers',
    columns=['customer_id', 'name', 'email'],
    key_columns=['customer_id'],
    update_columns=['name', 'email']
)

# MERGE operation
merge_sql = merge_statement(
    target_schema='silver',
    target_table='customers',
    source_query='SELECT * FROM bronze.crm_customers',
    key_columns=['customer_id'],
    insert_columns=['customer_id', 'name', 'email'],
    update_columns=['name', 'email']
)
```

**Key Functions**:
- `bulk_insert()` - Efficient bulk inserts
- `upsert()` - INSERT ... ON CONFLICT
- `soft_delete()` - Soft delete operations
- `batch_update()` - Batch updates
- `merge_statement()` - MERGE-like operations with CTEs
- `incremental_load()` - Incremental data loading
- `generate_copy_statement()` - COPY for CSV imports

**Integration**:
- Used by ETL processes
- Supports medallion patterns
- Handles metadata columns automatically

### `sql/query_builder.py`
**Query Building Utilities**

Low-level builders for constructing SQL queries:

```python
from sql.query_builder import (
    select_builder,
    window_function_builder,
    cte_builder
)

# SELECT query
query = select_builder(
    schema='silver',
    table='customers',
    columns=['customer_id', 'name', 'email'],
    where_conditions=['status = :status'],
    order_by=['created_at DESC'],
    limit=100
)

# Window function
window_sql = window_function_builder(
    function_name='ROW_NUMBER',
    partition_by=['customer_id'],
    order_by=['created_at DESC'],
    alias='row_num'
)

# Common Table Expression
cte_sql = cte_builder(
    cte_name='recent_customers',
    cte_query='SELECT * FROM silver.customers WHERE created_at > NOW() - INTERVAL \'30 days\''
)
```

**Key Builders**:
- `select_builder()` - SELECT statements
- `join_builder()` - JOIN clauses
- `where_builder()` - WHERE conditions
- `pagination_builder()` - LIMIT/OFFSET
- `cte_builder()` - Common Table Expressions
- `window_function_builder()` - Window functions
- `subquery_builder()` - Subqueries
- `recursive_cte_builder()` - Recursive CTEs

**Metadata Queries**:
- `check_schema_exists_sql()` - Schema existence
- `get_schema_info_sql()` - Schema metadata
- `get_table_info_sql()` - Table metadata
- `get_column_info_sql()` - Column metadata
- `get_database_info_sql()` - Database info
- `get_table_stats_sql()` - Table statistics

**Integration**:
- Used by [`common_queries`](#sqlcommon_queriespy)
- Used by [`create_schemas`](#setupcreate_schemaspy)
- Foundation for all query construction

### `sql/common_queries.py`
**Common Query Patterns**

High-level query patterns for common use cases:

```python
from sql.common_queries import (
    analyze_medallion_layer,
    trace_data_lineage,
    check_data_quality
)

# Analyze medallion layer
query = analyze_medallion_layer(
    layer='silver',
    table='customers',
    business_date='2024-01-01',
    source_system='crm'
)

# Trace data lineage
lineage_query = trace_data_lineage(
    target_schema='gold',
    target_table='customer_analytics',
    include_upstream=True,
    max_depth=3
)

# Data quality checks
quality_query = check_data_quality(
    schema='silver',
    table='customers',
    checks=[
        {'column': 'email', 'check_type': 'not_null'},
        {'column': 'customer_id', 'check_type': 'unique'}
    ]
)
```

**Key Functions**:
- `analyze_medallion_layer()` - Query medallion layers
- `trace_data_lineage()` - Lineage tracing
- `check_data_quality()` - Data quality validation
- `compute_pivot_table()` - Pivot operations
- `compute_running_totals()` - Running calculations
- `analyze_lag_lead()` - Time series analysis
- `analyze_cohort_retention()` - Cohort analysis
- `detect_time_series_gaps()` - Gap detection

**Integration**:
- Uses [`query_builder`](#sqlquery_builderpy) internally
- Provides medallion-optimized patterns
- Supports audit column filtering

## 📝 Logging Infrastructure

### `logs/audit_logger.py`
**Process and Configuration Auditing**

Track ETL process execution and configuration changes:

```python
from logs import ProcessLogger, ConfigurationLogger

# Process logging
process_logger = ProcessLogger(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

process_id = process_logger.start_process(
    process_name='bronze_ingestion',
    process_description='Load CRM data',
    source_system='CRM',
    target_layer='bronze'
)

# ... do ETL work ...

process_logger.end_process(
    log_id=process_id,
    status='SUCCESS',
    rows_processed=1000,
    error_message=None
)

# Configuration logging
config_logger = ConfigurationLogger()
config_logger.log_config_change(
    parameter_name='batch_size',
    old_value='1000',
    new_value='5000',
    change_reason='Performance optimization'
)
```

**Key Classes**:
- `ProcessLogger` - ETL process lifecycle tracking
- `ConfigurationLogger` - Configuration change auditing
- `BatchLogger` - Specialized batch operation logging

**Integration**:
- Uses [`create_logs`](#setupcreate_logspy) table models
- Called by [`setup_orchestrator`](#setupsetup_orchestratorpy)
- Foundation for compliance and debugging

### `logs/data_lineage.py`
**Data Lineage Tracking and Analysis**

Track data flow through medallion architecture:

```python
from logs import LineageTracker, LineageAnalyzer, ImpactAnalyzer

# Track lineage
tracker = LineageTracker(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

lineage_id = tracker.log_lineage(
    process_log_id=123,
    source_schema='bronze',
    source_table='crm_customers',
    target_schema='silver',
    target_table='customers',
    transformation_logic='Data cleansing and standardization',
    rows_read=1000,
    rows_written=950
)

# Analyze lineage
analyzer = LineageAnalyzer()
upstream = analyzer.get_upstream_lineage('gold', 'customer_analytics', max_depth=5)
downstream = analyzer.get_downstream_lineage('bronze', 'crm_customers', max_depth=5)
medallion_flow = analyzer.get_medallion_flow()

# Impact analysis
impact_analyzer = ImpactAnalyzer(analyzer)
impact = impact_analyzer.analyze_impact(
    changed_schema='silver',
    changed_table='customers',
    change_type='SCHEMA_CHANGE'
)
```

**Key Classes**:
- `LineageTracker` - Record data transformations
- `LineageAnalyzer` - Analyze lineage relationships
- `ImpactAnalyzer` - Assess downstream impact

**Key Features**:
- Complete lineage tracking across layers
- Transformation logic documentation
- Source-to-target mapping
- Impact analysis for changes
- Critical path identification

**Integration**:
- Uses [`create_logs`](#setupcreate_logspy) table models
- Supports compliance and governance
- Enables root cause analysis

### `logs/error_handler.py`
**Error Logging and Recovery**

Centralized error handling with recovery mechanisms:

```python
from logs import ErrorLogger, ErrorRecovery

# Error logging
error_logger = ErrorLogger(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

error_logger.log_error(
    process_log_id=123,
    error_message="Validation failed: Invalid email format",
    error_code="DATA_VALIDATION_ERROR",
    error_level="ERROR",
    affected_table="bronze.crm_customers",
    recovery_suggestion="Check source data format"
)

# Error recovery
recovery = ErrorRecovery()
recovery_plan = recovery.get_recovery_plan('DATA_VALIDATION_ERROR')
success = recovery.attempt_recovery(error_id=456, max_retries=3)
```

**Key Classes**:
- `ErrorLogger` - Centralized error logging
- `ErrorRecovery` - Automated recovery mechanisms
- `ErrorAnalyzer` - Error pattern analysis

**Error Levels**:
- `INFO` - Informational messages
- `WARNING` - Warnings (not critical)
- `ERROR` - Errors requiring attention
- `CRITICAL` - Critical failures

**Integration**:
- Uses [`create_logs`](#setupcreate_logspy) table models
- Provides recovery suggestions
- Supports error trend analysis

### `logs/performance_monitor.py`
**Performance Monitoring**

Track and analyze performance metrics:

```python
from logs import PerformanceMonitor, MetricsCollector

# Performance monitoring
monitor = PerformanceMonitor(
    host='localhost',
    user='postgres',
    password='password',
    database='warehouse'
)

# Record metrics
monitor.record_metric(
    process_log_id=123,
    metric_name='processing_time',
    metric_value=45.2,
    metric_unit='seconds'
)

monitor.record_metric(
    process_log_id=123,
    metric_name='memory_usage',
    metric_value=512.5,
    metric_unit='MB'
)

# Context manager for automatic timing
with monitor.track_performance(process_log_id=123, operation_name='data_load'):
    # ... do work ...
    pass

# Collect and analyze metrics
collector = MetricsCollector()
summary = collector.get_performance_summary(
    process_name='bronze_ingestion',
    days=7
)
```

**Key Classes**:
- `PerformanceMonitor` - Track performance metrics
- `MetricsCollector` - Collect and aggregate metrics
- `ThroughputAnalyzer` - Analyze data processing throughput

**Key Features**:
- Execution time tracking
- Resource usage monitoring (CPU, memory)
- Custom metric recording
- Context manager for automatic timing
- Performance trend analysis

**Integration**:
- Uses [`create_logs`](#setupcreate_logspy) table models
- Uses `psutil` for system metrics
- Supports SLA monitoring

## ⚙️ Configuration

### Environment Variables (`.env`)

```env
# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Database Names
POSTGRES_DB=postgres
WAREHOUSE_DB=sql_retail_analytics_warehouse
```

### Configuration Access

```python
from core.config import config

# Database connection settings
host = config.db_host
port = config.db_port
user = config.db_user
password = config.db_password
admin_db = config.db_name
warehouse_db = config.warehouse_db_name

# Connection string
conn_str = config.get_connection_string(use_warehouse=True)

# Project paths
data_dir = config.project.data_dir
logs_dir = config.project.logs_dir
medallion_dir = config.project.medallion_dir
```

## 💡 Usage Examples

### Complete Setup

```python
from setup import SetupOrchestrator

orchestrator = SetupOrchestrator()
results = orchestrator.run_complete_setup(include_samples=True)

for step, success in results.items():
    print(f"{step}: {'✓' if success else '✗'}")
```

### ETL Process with Logging

```python
from logs import ProcessLogger, PerformanceMonitor, LineageTracker

# Initialize loggers
process_logger = ProcessLogger()
perf_monitor = PerformanceMonitor()
lineage = LineageTracker()

# Start process
process_id = process_logger.start_process(
    process_name='bronze_to_silver',
    process_description='Transform bronze data to silver',
    source_system='bronze',
    target_layer='silver'
)

try:
    # Track performance
    with perf_monitor.track_performance(process_id, 'transformation'):
        # ... ETL logic ...
        rows_processed = 1000
    
    # Log lineage
    lineage.log_lineage(
        process_log_id=process_id,
        source_schema='bronze',
        source_table='crm_customers',
        target_schema='silver',
        target_table='customers',
        transformation_logic='Data cleansing and validation',
        rows_read=1000,
        rows_written=950
    )
    
    # End process
    process_logger.end_process(
        log_id=process_id,
        status='SUCCESS',
        rows_processed=rows_processed
    )

except Exception as e:
    # Log error
    from logs import ErrorLogger
    error_logger = ErrorLogger()
    error_logger.log_error(
        process_log_id=process_id,
        error_message=str(e),
        error_level='ERROR'
    )
    
    # End process with failure
    process_logger.end_process(
        log_id=process_id,
        status='FAILED',
        error_message=str(e)
    )
```

### Data Quality Checks

```python
from sql.common_queries import check_data_quality
from sqlalchemy import create_engine, text
from core.config import config

# Define quality checks
checks = [
    {'column': 'email', 'check_type': 'not_null'},
    {'column': 'email', 'check_type': 'format', 'pattern': '%@%.%'},
    {'column': 'customer_id', 'check_type': 'unique'},
    {'column': 'registration_date', 'check_type': 'range', 'min': '2020-01-01'}
]

# Generate and execute quality check query
quality_sql = check_data_quality(
    schema='silver',
    table='customers',
    checks=checks
)

engine = create_engine(config.get_connection_string(use_warehouse=True))
with engine.connect() as conn:
    results = conn.execute(text(quality_sql)).fetchall()
```

### Impact Analysis

```python
from logs.data_lineage import LineageAnalyzer, ImpactAnalyzer

# Initialize analyzers
lineage_analyzer = LineageAnalyzer()
impact_analyzer = ImpactAnalyzer(lineage_analyzer)

# Analyze impact of schema change
impact = impact_analyzer.analyze_impact(
    changed_schema='silver',
    changed_table='customers',
    change_type='SCHEMA_CHANGE'
)

print(f"Impact Severity: {impact['impact_severity']}")
print(f"Affected Tables: {impact['downstream']['total_downstream_tables']}")
print(f"Critical Paths: {len(impact['critical_paths'])}")
print("\nRecommendations:")
for rec in impact['recommendations']:
    print(f"  - {rec}")
```

## 🛠️ Development

### Project Dependencies

```txt
# Database
psycopg2-binary==2.9.10  # PostgreSQL adapter
SQLAlchemy==2.0.44       # ORM and database toolkit

# Configuration
python-dotenv==1.2.1     # Environment variable management

# Monitoring
psutil==7.1.3            # System and process monitoring

# Utilities
typing_extensions==4.15.0  # Type hint extensions
greenlet==3.2.4          # Coroutine support (SQLAlchemy dependency)
```

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test module
python -m pytest tests/test_setup.py

# Run with coverage
python -m pytest --cov=. tests/
```

### Code Style

The project follows Python best practices:
- PEP 8 style guide
- Type hints for function signatures
- Comprehensive docstrings
- Modular, reusable code

### Adding New Modules

1. Create module in appropriate package (`core/`, `logs/`, `sql/`, `setup/`)
2. Add exports to package `__init__.py`
3. Update this README with module documentation
4. Add tests in `tests/` directory

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🎓 Learning Resources

This project demonstrates:

- **Medallion Architecture**: Multi-layered data warehouse design
- **SQLAlchemy ORM**: Type-safe database operations
- **Enterprise Patterns**: Audit trails, data lineage, error handling
- **Modular Design**: Reusable, maintainable code
- **Configuration Management**: Environment-based settings
- **Performance Monitoring**: Metrics and optimization

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📧 Support

For questions or issues, please open an issue on the repository.

---

**Built with ❤️ using Python, PostgreSQL, and SQLAlchemy**