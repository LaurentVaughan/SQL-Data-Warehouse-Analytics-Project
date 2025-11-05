# Bronze Layer Implementation Plan

## 📋 Overview

The Bronze Layer is the **raw data ingestion layer** in the Medallion Architecture. It serves as the landing zone for data from source systems (CRM and ERP) with minimal transformation.

**Objective**: Create a robust, scalable bronze layer that:
- Ingests raw data from CSV sources
- Preserves complete data history
- Implements audit logging and lineage tracking
- Handles errors gracefully with comprehensive logging
- Supports incremental and full load patterns

## 🎯 Current State

### Completed (Phase 1)
✅ Database infrastructure (`setup/create_database.py`)
✅ Schema infrastructure (`setup/create_schemas.py`)
✅ Logging infrastructure with 4 modules:
  - `logs/audit_logger.py` - Process tracking
  - `logs/error_handler.py` - Error logging and recovery
  - `logs/performance_monitor.py` - Performance metrics
  - `logs/data_lineage.py` - Lineage tracking
✅ Centralized ORM models (`models/logs_models.py`)
✅ Comprehensive test suite (207 passing tests)

### Available Resources
- **Source Data**:
  - CRM: `datasets/source_crm/` (cust_info.csv, prd_info.csv, sales_details.csv)
  - ERP: `datasets/source_erp/` (CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv)
- **Bronze Schema**: Already created in database
- **Logging Infrastructure**: Ready for integration

## 🏗️ Architecture Design

### Bronze Layer Principles

1. **Append-Only Storage**: Never delete or update existing records
2. **Schema-on-Read**: Minimal validation, preserve source data fidelity
3. **Full Audit Trail**: Log every load operation
4. **Partition by Time**: Enable efficient querying and archiving
5. **Metadata Enrichment**: Add ingestion timestamps, source identifiers

### Data Flow

```
Source Systems (CSV)
        ↓
    Validation
        ↓
Bronze Tables (bronze.*)
        ↓
Audit Logs (logs.process_log)
        ↓
Lineage Tracking (logs.data_lineage)
```

## 📊 Bronze Table Design

### Table Naming Convention

`bronze.<source_system>_<entity>_raw`

Examples:
- `bronze.crm_customers_raw`
- `bronze.crm_products_raw`
- `bronze.crm_sales_raw`
- `bronze.erp_customers_raw`
- `bronze.erp_locations_raw`
- `bronze.erp_product_categories_raw`

### Standard Bronze Table Structure

Every bronze table includes:

**Source Columns**: All columns from source (preserved as-is)
**Metadata Columns**:
- `_bronze_id` (BIGSERIAL PRIMARY KEY) - Surrogate key
- `_ingestion_timestamp` (TIMESTAMP) - When record was loaded
- `_ingestion_batch_id` (VARCHAR(100)) - Batch identifier
- `_source_file` (VARCHAR(500)) - Source filename
- `_source_row_number` (INTEGER) - Original row number in source
- `_is_current` (BOOLEAN) - Latest version flag (for CDC scenarios)
- `_hash` (VARCHAR(64)) - SHA256 hash of source data (for change detection)

## 📁 Implementation Structure

```
medallion/
└── bronze/
    ├── __init__.py
    ├── base_loader.py           # Abstract base class for all loaders
    ├── csv_loader.py            # CSV file ingestion utilities
    ├── validators.py            # Data validation rules
    ├── transformers.py          # Minimal transformations (type casting)
    ├── loaders/
    │   ├── __init__.py
    │   ├── crm_loader.py        # CRM-specific loading logic
    │   └── erp_loader.py        # ERP-specific loading logic
    └── models/
        ├── __init__.py
        └── bronze_models.py     # Bronze layer ORM models
```

## 🔧 Core Components

### 1. `bronze/base_loader.py`

Abstract base class providing common functionality:

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from pathlib import Path
import hashlib
from datetime import datetime
from logs import ProcessLogger, ErrorLogger, PerformanceMonitor, LineageTracker

class BronzeLoader(ABC):
    """Base class for all bronze layer loaders."""
    
    def __init__(self, host: str, user: str, password: str, database: str):
        self.connection_params = {...}
        self.process_logger = ProcessLogger(...)
        self.error_logger = ErrorLogger(...)
        self.perf_monitor = PerformanceMonitor(...)
        self.lineage_tracker = LineageTracker(...)
        
    @abstractmethod
    def get_table_name(self) -> str:
        """Return bronze table name."""
        pass
    
    @abstractmethod
    def get_source_columns(self) -> List[str]:
        """Return list of expected source columns."""
        pass
    
    def load_csv(
        self,
        file_path: Path,
        batch_id: str,
        validate: bool = True
    ) -> Dict[str, int]:
        """Load CSV file into bronze table."""
        pass
    
    def validate_schema(self, df) -> bool:
        """Validate source data schema."""
        pass
    
    def enrich_metadata(self, df, batch_id: str, file_path: Path):
        """Add bronze metadata columns."""
        pass
    
    def calculate_hash(self, row_data: str) -> str:
        """Calculate SHA256 hash of row data."""
        return hashlib.sha256(row_data.encode()).hexdigest()
    
    def log_lineage(self, source: str, target: str, rows: int):
        """Log data lineage information."""
        pass
```

**Key Features**:
- Template method pattern for consistent loading
- Integrated logging at every step
- Performance monitoring built-in
- Automatic lineage tracking
- Hash-based change detection

### 2. `bronze/csv_loader.py`

CSV-specific utilities:

```python
import pandas as pd
from typing import Optional
from pathlib import Path

class CSVLoader:
    """Utilities for reading CSV files."""
    
    @staticmethod
    def read_csv(
        file_path: Path,
        encoding: str = 'utf-8',
        delimiter: str = ',',
        **kwargs
    ) -> pd.DataFrame:
        """Read CSV with error handling."""
        pass
    
    @staticmethod
    def infer_types(df: pd.DataFrame) -> Dict[str, str]:
        """Infer SQL types from pandas DataFrame."""
        pass
    
    @staticmethod
    def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 10000):
        """Yield dataframe in chunks for memory efficiency."""
        pass
```

**Key Features**:
- Robust CSV reading with encoding detection
- Type inference for table creation
- Chunking for large files
- Memory-efficient processing

### 3. `bronze/validators.py`

Data validation framework:

```python
from typing import List, Dict, Callable
from dataclasses import dataclass

@dataclass
class ValidationRule:
    """Single validation rule."""
    name: str
    check: Callable
    severity: str  # 'error', 'warning', 'info'
    message: str

class DataValidator:
    """Validate data quality before loading."""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule):
        """Add validation rule."""
        pass
    
    def validate(self, df) -> Dict[str, List[str]]:
        """Run all validation rules."""
        pass
    
    # Standard validation rules
    @staticmethod
    def check_not_null(column: str) -> ValidationRule:
        """Ensure column has no null values."""
        pass
    
    @staticmethod
    def check_unique(column: str) -> ValidationRule:
        """Ensure column values are unique."""
        pass
    
    @staticmethod
    def check_data_type(column: str, expected_type: str) -> ValidationRule:
        """Ensure column matches expected type."""
        pass
```

**Key Features**:
- Declarative validation rules
- Severity levels (error vs warning)
- Extensible validation framework
- Integration with error logging

### 4. `bronze/loaders/crm_loader.py`

CRM-specific implementation:

```python
from bronze.base_loader import BronzeLoader
from bronze.validators import DataValidator, ValidationRule

class CRMCustomerLoader(BronzeLoader):
    """Load CRM customer data into bronze.crm_customers_raw."""
    
    def get_table_name(self) -> str:
        return "bronze.crm_customers_raw"
    
    def get_source_columns(self) -> List[str]:
        return ["customer_id", "customer_name", "email", "phone", ...]
    
    def get_validators(self) -> DataValidator:
        """Define CRM customer-specific validation rules."""
        validator = DataValidator()
        validator.add_rule(
            ValidationRule.check_not_null("customer_id")
        )
        validator.add_rule(
            ValidationRule.check_unique("customer_id")
        )
        return validator

class CRMProductLoader(BronzeLoader):
    """Load CRM product data into bronze.crm_products_raw."""
    # Similar structure...

class CRMSalesLoader(BronzeLoader):
    """Load CRM sales data into bronze.crm_sales_raw."""
    # Similar structure...
```

### 5. `bronze/models/bronze_models.py`

ORM definitions for bronze tables:

```python
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class BronzeBase:
    """Mixin for bronze metadata columns."""
    _bronze_id = Column(BigInteger, primary_key=True, autoincrement=True)
    _ingestion_timestamp = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    _ingestion_batch_id = Column(String(100), nullable=False)
    _source_file = Column(String(500), nullable=False)
    _source_row_number = Column(Integer, nullable=False)
    _is_current = Column(Boolean, nullable=False, default=True)
    _hash = Column(String(64), nullable=False)

class CRMCustomerRaw(Base, BronzeBase):
    __tablename__ = 'crm_customers_raw'
    __table_args__ = {'schema': 'bronze'}
    
    # Source columns
    customer_id = Column(String(50), nullable=False)
    customer_name = Column(String(200))
    email = Column(String(200))
    phone = Column(String(50))
    # ... other source columns

class CRMProductRaw(Base, BronzeBase):
    __tablename__ = 'crm_products_raw'
    __table_args__ = {'schema': 'bronze'}
    # Similar structure...

# Additional models for other bronze tables...
```

## 📝 Implementation Steps

### Phase 1: Foundation (Week 1)

**Tasks**:
1. Create `bronze/__init__.py` with package exports
2. Implement `bronze/base_loader.py` with abstract base class
3. Implement `bronze/csv_loader.py` with CSV utilities
4. Implement `bronze/validators.py` with validation framework
5. Create `bronze/models/bronze_models.py` with ORM definitions

**Deliverables**:
- ✅ Base infrastructure ready
- ✅ CSV reading and chunking working
- ✅ Validation framework operational
- ✅ Unit tests for all components (target: 50+ tests)

### Phase 2: CRM Data Loaders (Week 2)

**Tasks**:
1. Implement `CRMCustomerLoader` for customer data
2. Implement `CRMProductLoader` for product data
3. Implement `CRMSalesLoader` for sales data
4. Create DDL scripts for bronze table creation
5. Integration testing with actual CSV files

**Deliverables**:
- ✅ All CRM data loading into bronze
- ✅ Audit logs capturing all operations
- ✅ Lineage tracking source → bronze
- ✅ Integration tests (target: 30+ tests)

### Phase 3: ERP Data Loaders (Week 3)

**Tasks**:
1. Implement `ERPCustomerLoader` (CUST_AZ12.csv)
2. Implement `ERPLocationLoader` (LOC_A101.csv)
3. Implement `ERPProductCategoryLoader` (PX_CAT_G1V2.csv)
4. Handle ERP-specific naming conventions
5. Cross-system validation (CRM vs ERP customer matching)

**Deliverables**:
- ✅ All ERP data loading into bronze
- ✅ Cross-system validation rules
- ✅ Integration tests (target: 30+ tests)

### Phase 4: Incremental Loading & Error Recovery (Week 4)

**Tasks**:
1. Implement incremental load detection (hash-based)
2. Implement CDC (Change Data Capture) pattern with `_is_current` flag
3. Error recovery mechanisms (retry logic, circuit breaker)
4. Performance optimization (bulk inserts, indexing)
5. Load orchestration script

**Deliverables**:
- ✅ Incremental loads working
- ✅ Error recovery tested
- ✅ Performance benchmarks established
- ✅ End-to-end orchestration script

## 🔗 Integration Points

### Logging Integration

Every bronze load operation logs:

1. **Process Log** (via `ProcessLogger`):
   ```python
   process_id = process_logger.start_process(
       process_name="bronze_crm_customer_load",
       layer="bronze",
       metadata={"source_file": file_path, "batch_id": batch_id}
   )
   ```

2. **Error Log** (via `ErrorLogger`):
   ```python
   error_logger.log_error(
       process_id=process_id,
       error_type="ValidationError",
       error_message="Customer ID cannot be null",
       severity="high"
   )
   ```

3. **Performance Metrics** (via `PerformanceMonitor`):
   ```python
   with perf_monitor.monitor_process("csv_read"):
       df = pd.read_csv(file_path)
   
   perf_monitor.record_metric(
       process_id=process_id,
       metric_name="rows_loaded",
       metric_value=len(df)
   )
   ```

4. **Data Lineage** (via `LineageTracker`):
   ```python
   lineage_tracker.log_lineage(
       process_id=process_id,
       source_schema="file_system",
       source_table=file_path.name,
       target_schema="bronze",
       target_table="crm_customers_raw",
       transformation_logic="CSV direct load with metadata enrichment"
   )
   ```

### SQL Utilities Integration

Use existing SQL modules:

```python
from sql.ddl import DDLGenerator
from sql.dml import DMLGenerator

# Create bronze tables
ddl = DDLGenerator()
create_table_sql = ddl.create_table_from_model(CRMCustomerRaw)

# Insert data
dml = DMLGenerator()
insert_sql = dml.bulk_insert("bronze.crm_customers_raw", df.to_dict('records'))
```

## 🧪 Testing Strategy

### Test Structure

```
tests/
├── tests_bronze/
│   ├── __init__.py
│   ├── conftest.py                    # Shared fixtures
│   ├── test_base_loader.py            # Base class tests (15 tests)
│   ├── test_csv_loader.py             # CSV utilities (10 tests)
│   ├── test_validators.py             # Validation framework (20 tests)
│   ├── test_crm_loaders.py            # CRM loaders (30 tests)
│   ├── test_erp_loaders.py            # ERP loaders (30 tests)
│   ├── test_bronze_models.py          # ORM models (10 tests)
│   └── test_integration.py            # End-to-end tests (15 tests)
```

**Target**: 130+ new tests (total project: 337 tests)

### Test Categories

1. **Unit Tests**: Test individual loader methods
2. **Integration Tests**: Test full CSV → Bronze flow
3. **Validation Tests**: Test all validation rules
4. **Error Tests**: Test error handling and recovery
5. **Performance Tests**: Test bulk loading performance

## 📊 Success Criteria

### Functional Requirements

- ✅ All 6 CSV files successfully load into bronze tables
- ✅ All source columns preserved exactly
- ✅ Metadata columns correctly populated
- ✅ Hash-based change detection working
- ✅ Incremental loads only insert new/changed records
- ✅ Validation errors logged but don't stop load

### Non-Functional Requirements

- ✅ Load performance: >10,000 rows/second
- ✅ Memory efficiency: Process files >1GB without OOM
- ✅ Test coverage: >90% for bronze package
- ✅ All operations logged in audit tables
- ✅ Complete lineage tracking

### Quality Metrics

- All tests passing (target: 337 total)
- No circular import dependencies
- Type hints on all public methods
- Comprehensive docstrings
- Error handling on all I/O operations

## 🎯 Next Steps (After Bronze Completion)

1. **Silver Layer**: Cleansing, deduplication, standardization
2. **Gold Layer**: Business aggregations and KPIs
3. **Orchestration**: Apache Airflow DAGs for automated pipelines
4. **Monitoring**: Dashboards for data quality and lineage
5. **CI/CD**: Automated testing and deployment

## 📚 References

- [PHASE1_COMPLETE.md](PHASE1_COMPLETE.md) - Infrastructure foundation
- [REFACTORING_PLAN.md](REFACTORING_PLAN.md) - Technical architecture details
- [README.md](README.md) - Project overview and setup

---

**Author**: SQL Data Warehouse Analytics Team  
**Version**: 1.0  
**Last Updated**: 2025-01-XX (To be filled on commit)
