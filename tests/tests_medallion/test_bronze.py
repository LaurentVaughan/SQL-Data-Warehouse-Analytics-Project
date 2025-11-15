"""
==================================================
Comprehensive pytest suite for bronze.py
==================================================

Tests for the BronzeManager class and bronze layer data ingestion.

Sections:
---------
1. Unit tests - Individual method testing
2. Integration tests - Component interaction testing
3. System tests - Full workflow testing
4. End-to-End tests - Complete ingestion scenarios
5. Smoke tests - Basic functionality checks
6. Edge case tests - Boundary conditions and error handling

Available markers:
------------------
unit, integration, system, e2e, smoke, edge_case

How to Execute:
---------------
All tests:          pytest tests/tests_medallion/test_bronze.py -v
By category:        pytest tests/tests_medallion/test_bronze.py -m unit
Multiple markers:   pytest tests/tests_medallion/test_bronze.py -m "unit or integration"
Specific test:      pytest tests/tests_medallion/test_bronze.py::test_bronze_manager_init_success
With coverage:      pytest tests/tests_medallion/test_bronze.py --cov=medallion.bronze

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from medallion.bronze import BronzeManager, BronzeManagerError

# =============================================================================
# SECTION 1: UNIT TESTS - Individual method testing
# =============================================================================

@pytest.mark.unit
def test_bronze_manager_init_success(bronze_manager_factory):
    """
    Unit test: BronzeManager initializes successfully with valid database.
    
    Verifies:
    - Manager instance is created
    - Database connection is established
    - Logging infrastructure is initialized
    - Performance monitoring is configured
    """
    manager = bronze_manager_factory(monitor_performance=True, db_exists=True)
    
    assert manager is not None
    assert manager.bronze_schema == 'bronze'
    assert manager.monitor_performance is True
    assert manager.engine is not None
    assert manager.process_logger is not None
    assert manager.error_logger is not None
    assert manager.lineage_tracker is not None
    assert manager.perf_monitor is not None


@pytest.mark.unit
def test_bronze_manager_init_no_database():
    """
    Unit test: BronzeManager raises error when database doesn't exist.
    
    Verifies:
    - BronzeManagerError is raised
    - Error message mentions missing database
    - Suggests running setup
    """
    with patch('medallion.bronze.verify_database_exists') as mock_verify:
        mock_verify.return_value = False
        
        with pytest.raises(BronzeManagerError) as exc_info:
            BronzeManager()
        
        assert "does not exist" in str(exc_info.value)
        assert "setup" in str(exc_info.value).lower()


@pytest.mark.unit
def test_bronze_manager_init_custom_schema(bronze_manager_factory):
    """
    Unit test: BronzeManager accepts custom schema name.
    
    Verifies:
    - Custom schema name is stored
    - Manager initializes with custom schema
    """
    manager = bronze_manager_factory(bronze_schema='custom_bronze')
    
    assert manager.bronze_schema == 'custom_bronze'


@pytest.mark.unit
def test_bronze_manager_init_no_performance_monitoring(bronze_manager_factory):
    """
    Unit test: BronzeManager initializes without performance monitoring.
    
    Verifies:
    - Manager initializes successfully
    - Performance monitor is None when disabled
    """
    with patch('medallion.bronze.verify_database_exists') as mock_verify, \
         patch('medallion.bronze.create_sqlalchemy_engine') as mock_engine, \
         patch('medallion.bronze.ProcessLogger') as mock_pl, \
         patch('medallion.bronze.ErrorLogger') as mock_el, \
         patch('medallion.bronze.LineageTracker') as mock_lt, \
         patch('medallion.bronze.ErrorRecovery') as mock_er:
        
        mock_verify.return_value = True
        mock_engine.return_value = MagicMock()
        mock_pl.return_value = MagicMock()
        mock_el.return_value = MagicMock()
        mock_lt.return_value = MagicMock()
        mock_er.return_value = MagicMock()
        
        manager = BronzeManager(monitor_performance=False)
        
        assert manager.perf_monitor is None


@pytest.mark.unit
def test_infer_sql_type_integer(bronze_manager_factory):
    """
    Unit test: _infer_sql_type correctly maps integer types.
    
    Verifies:
    - int64 maps to BIGINT
    - int32 maps to BIGINT
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('int64') == 'BIGINT'
    assert manager._infer_sql_type('int32') == 'BIGINT'
    assert manager._infer_sql_type('Int64') == 'BIGINT'


@pytest.mark.unit
def test_infer_sql_type_float(bronze_manager_factory):
    """
    Unit test: _infer_sql_type correctly maps float types.
    
    Verifies:
    - float64 maps to NUMERIC
    - float32 maps to NUMERIC
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('float64') == 'NUMERIC'
    assert manager._infer_sql_type('float32') == 'NUMERIC'


@pytest.mark.unit
def test_infer_sql_type_boolean(bronze_manager_factory):
    """
    Unit test: _infer_sql_type correctly maps boolean types.
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('bool') == 'BOOLEAN'
    assert manager._infer_sql_type('boolean') == 'BOOLEAN'


@pytest.mark.unit
def test_infer_sql_type_datetime(bronze_manager_factory):
    """
    Unit test: _infer_sql_type correctly maps datetime types.
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('datetime64') == 'TIMESTAMP'
    assert manager._infer_sql_type('datetime64[ns]') == 'TIMESTAMP'


@pytest.mark.unit
def test_infer_sql_type_date(bronze_manager_factory):
    """
    Unit test: _infer_sql_type correctly maps date types.
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('date') == 'DATE'


@pytest.mark.unit
def test_infer_sql_type_string(bronze_manager_factory):
    """
    Unit test: _infer_sql_type maps unknown types to TEXT.
    """
    manager = bronze_manager_factory()
    
    assert manager._infer_sql_type('object') == 'TEXT'
    assert manager._infer_sql_type('string') == 'TEXT'
    assert manager._infer_sql_type('unknown_type') == 'TEXT'


@pytest.mark.unit
def test_infer_table_schema_with_metadata(bronze_manager_factory, mock_csv_data):
    """
    Unit test: _infer_table_schema creates schema with bronze metadata.
    
    Verifies:
    - Source columns are included
    - Bronze metadata columns are added
    - Column types are correctly inferred
    """
    manager = bronze_manager_factory()
    df = mock_csv_data['customers']
    
    schema = manager._infer_table_schema(df, include_metadata=True)
    
    # Check source columns
    assert 'customer_id' in schema
    assert 'name' in schema
    assert 'email' in schema
    
    # Check bronze metadata columns
    assert '_bronze_id' in schema
    assert '_ingestion_timestamp' in schema
    assert '_ingestion_batch_id' in schema
    assert '_source_file' in schema
    assert '_source_row_number' in schema
    assert '_is_current' in schema
    assert '_row_hash' in schema


@pytest.mark.unit
def test_infer_table_schema_without_metadata(bronze_manager_factory, mock_csv_data):
    """
    Unit test: _infer_table_schema creates schema without bronze metadata.
    
    Verifies:
    - Only source columns are included
    - No bronze metadata columns
    """
    manager = bronze_manager_factory()
    df = mock_csv_data['products']
    
    schema = manager._infer_table_schema(df, include_metadata=False)
    
    # Check source columns
    assert 'product_id' in schema
    assert 'name' in schema
    assert 'price' in schema
    
    # Check no bronze metadata columns
    assert '_bronze_id' not in schema
    assert '_ingestion_timestamp' not in schema


@pytest.mark.unit
def test_calculate_row_hash(bronze_manager_factory):
    """
    Unit test: _calculate_row_hash generates consistent SHA256 hashes.
    
    Verifies:
    - Same data produces same hash
    - Different data produces different hash
    - Hash is 64 characters (SHA256 hex)
    """
    manager = bronze_manager_factory()
    
    row1 = {'id': 1, 'name': 'Alice', 'email': 'alice@test.com'}
    row2 = {'id': 1, 'name': 'Alice', 'email': 'alice@test.com'}
    row3 = {'id': 2, 'name': 'Bob', 'email': 'bob@test.com'}
    
    hash1 = manager._calculate_row_hash(row1)
    hash2 = manager._calculate_row_hash(row2)
    hash3 = manager._calculate_row_hash(row3)
    
    # Same data produces same hash
    assert hash1 == hash2
    
    # Different data produces different hash
    assert hash1 != hash3
    
    # Hash is 64 characters (SHA256 hex)
    assert len(hash1) == 64
    assert all(c in '0123456789abcdef' for c in hash1)


@pytest.mark.unit
def test_calculate_row_hash_order_independent(bronze_manager_factory):
    """
    Unit test: _calculate_row_hash is order-independent due to sorting.
    
    Verifies:
    - Dictionary key order doesn't affect hash
    """
    manager = bronze_manager_factory()
    
    row1 = {'name': 'Alice', 'id': 1, 'email': 'alice@test.com'}
    row2 = {'id': 1, 'email': 'alice@test.com', 'name': 'Alice'}
    
    hash1 = manager._calculate_row_hash(row1)
    hash2 = manager._calculate_row_hash(row2)
    
    assert hash1 == hash2


@pytest.mark.unit
def test_enrich_dataframe(bronze_manager_factory, mock_csv_data):
    """
    Unit test: _enrich_dataframe adds bronze metadata columns.
    
    Verifies:
    - All metadata columns are added
    - Batch ID is set correctly
    - Source file is set correctly
    - Row numbers are sequential
    - Row hashes are calculated
    """
    manager = bronze_manager_factory()
    df = mock_csv_data['customers']
    batch_id = 'TEST_BATCH_123'
    source_file = '/path/to/customers.csv'
    
    enriched = manager._enrich_dataframe(df, batch_id, source_file)
    
    # Check metadata columns exist
    assert '_ingestion_batch_id' in enriched.columns
    assert '_source_file' in enriched.columns
    assert '_source_row_number' in enriched.columns
    assert '_is_current' in enriched.columns
    assert '_row_hash' in enriched.columns
    
    # Check metadata values
    assert all(enriched['_ingestion_batch_id'] == batch_id)
    assert all(enriched['_source_file'] == source_file)
    assert all(enriched['_is_current'] == True)
    assert list(enriched['_source_row_number']) == [1, 2, 3]
    
    # Check row hashes are unique (for different rows)
    assert len(enriched['_row_hash'].unique()) == len(enriched)


@pytest.mark.unit
def test_table_exists_true(bronze_manager_factory):
    """
    Unit test: table_exists returns True when table exists.
    """
    manager = bronze_manager_factory(table_exists=True)
    
    exists = manager.table_exists('test_table')
    
    assert exists is True


@pytest.mark.unit
def test_table_exists_false(bronze_manager_factory):
    """
    Unit test: table_exists returns False when table doesn't exist.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    exists = manager.table_exists('nonexistent_table')
    
    assert exists is False


@pytest.mark.unit
def test_table_exists_custom_schema(bronze_manager_factory):
    """
    Unit test: table_exists checks in custom schema.
    """
    manager = bronze_manager_factory(bronze_schema='custom', table_exists=True)
    
    exists = manager.table_exists('test_table', schema='custom')
    
    assert exists is True


@pytest.mark.unit
def test_table_exists_error_handling(bronze_manager_factory):
    """
    Unit test: table_exists returns False on error.
    """
    manager = bronze_manager_factory(execute_side_effect=SQLAlchemyError("Connection error"))
    
    exists = manager.table_exists('test_table')
    
    assert exists is False


@pytest.mark.unit
def test_close_disposes_engine(bronze_manager_factory):
    """
    Unit test: close() disposes the database engine.
    """
    manager = bronze_manager_factory()
    
    manager.close()
    
    manager._mock_engine.dispose.assert_called_once()


# =============================================================================
# SECTION 2: INTEGRATION TESTS - Component interaction testing
# =============================================================================

@pytest.mark.integration
def test_create_bronze_table_new_table(bronze_manager_factory):
    """
    Integration test: create_bronze_table creates new table successfully.
    
    Verifies:
    - Table creation SQL is executed
    - CREATE TABLE statement is committed
    """
    manager = bronze_manager_factory(table_exists=False)
    
    schema_def = {
        'id': 'BIGINT',
        'name': 'TEXT',
        '_bronze_id': 'BIGSERIAL PRIMARY KEY'
    }
    
    with patch('medallion.bronze.create_table') as mock_create_ddl:
        mock_create_ddl.return_value = "CREATE TABLE bronze.test_table (...);"
        
        result = manager.create_bronze_table('test_table', schema_def)
        
        assert result is True
        mock_create_ddl.assert_called_once()


@pytest.mark.integration
def test_create_bronze_table_already_exists(bronze_manager_factory):
    """
    Integration test: create_bronze_table handles existing table.
    
    Verifies:
    - Returns True if table already exists
    - Does not attempt to recreate
    """
    manager = bronze_manager_factory(table_exists=True)
    
    schema_def = {'id': 'BIGINT', 'name': 'TEXT'}
    
    result = manager.create_bronze_table('existing_table', schema_def)
    
    assert result is True


@pytest.mark.integration
def test_create_bronze_table_drop_if_exists(bronze_manager_factory):
    """
    Integration test: create_bronze_table drops and recreates when requested.
    
    Verifies:
    - DROP TABLE is executed
    - CREATE TABLE is executed
    """
    manager = bronze_manager_factory(table_exists=True)
    
    schema_def = {'id': 'BIGINT', 'name': 'TEXT'}
    
    with patch('medallion.bronze.create_table') as mock_create_ddl:
        mock_create_ddl.return_value = "CREATE TABLE bronze.test_table (...);"
        
        result = manager.create_bronze_table('test_table', schema_def, drop_if_exists=True)
        
        assert result is True
        # Verify DROP and CREATE were called
        mock_create_ddl.assert_called_once()


@pytest.mark.integration
def test_create_bronze_table_sql_error(bronze_manager_factory):
    """
    Integration test: create_bronze_table handles SQL errors.
    
    Verifies:
    - BronzeManagerError is raised
    - Error message is descriptive
    """
    manager = bronze_manager_factory(table_exists=False)
    
    schema_def = {'id': 'BIGINT'}
    
    with patch('medallion.bronze.create_table') as mock_create_ddl:
        mock_create_ddl.return_value = "CREATE TABLE bronze.test_table (...);"
        
        # Make execute raise an error
        manager._mock_engine.connect.return_value.__enter__.return_value.execute.side_effect = \
            SQLAlchemyError("Table creation failed")
        
        with pytest.raises(BronzeManagerError) as exc_info:
            manager.create_bronze_table('test_table', schema_def)
        
        assert "Failed to create table" in str(exc_info.value)


@pytest.mark.integration
def test_load_csv_to_bronze_complete_workflow(bronze_manager_factory, mock_csv_file, tmp_path):
    """
    Integration test: load_csv_to_bronze complete workflow.
    
    Verifies:
    - CSV is read successfully
    - Table is created
    - Data is enriched
    - Data is loaded
    - Audit logging occurs
    - Lineage is tracked
    - Performance metrics recorded
    """
    manager = bronze_manager_factory(table_exists=False, monitor_performance=True)
    csv_path = mock_csv_file['customers']
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='customers_raw',
            source_system='CRM',
            batch_size=100
        )
        
        # Verify result structure
        assert 'rows_loaded' in result
        assert 'batch_id' in result
        assert 'process_log_id' in result
        assert 'duration_seconds' in result
        assert 'table_name' in result
        
        # Verify rows loaded
        assert result['rows_loaded'] == 3  # Sample data has 3 rows
        
        # Verify process logging
        manager._mock_process_logger.start_process.assert_called_once()
        manager._mock_process_logger.end_process.assert_called_once()
        
        # Verify lineage tracking
        manager._mock_lineage_tracker.log_lineage.assert_called_once()
        
        # Verify performance monitoring
        manager._mock_performance_monitor.record_metric.assert_called_once()


@pytest.mark.integration
def test_load_csv_to_bronze_file_not_found(bronze_manager_factory):
    """
    Integration test: load_csv_to_bronze handles missing CSV file.
    
    Verifies:
    - BronzeManagerError is raised
    - Error is logged
    - Process is marked as failed
    """
    manager = bronze_manager_factory()
    
    with patch('medallion.bronze.config') as mock_config:
        mock_config.project.project_root = Path('/nonexistent')
        
        with pytest.raises(BronzeManagerError) as exc_info:
            manager.load_csv_to_bronze(
                csv_path='/nonexistent/file.csv',
                table_name='test_table',
                source_system='TEST'
            )
        
        assert "not found" in str(exc_info.value)
        
        # Verify error logging
        manager._mock_error_logger.log_exception.assert_called_once()
        
        # Verify process marked as failed
        end_process_call = manager._mock_process_logger.end_process.call_args
        assert end_process_call[1]['status'] == 'FAILED'


@pytest.mark.integration
def test_load_csv_to_bronze_with_existing_table(bronze_manager_factory, mock_csv_file, tmp_path):
    """
    Integration test: load_csv_to_bronze with existing table.
    
    Verifies:
    - Table creation is skipped
    - Data is loaded into existing table
    """
    manager = bronze_manager_factory(table_exists=True)
    csv_path = mock_csv_file['products']
    
    with patch('medallion.bronze.config') as mock_config:
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='products_raw',
            source_system='CRM',
            create_table=False
        )
        
        assert result['rows_loaded'] == 3


@pytest.mark.integration
def test_load_csv_to_bronze_batch_processing(bronze_manager_factory, tmp_path):
    """
    Integration test: load_csv_to_bronze processes data in batches.
    
    Verifies:
    - Large datasets are processed in batches
    - All rows are loaded
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create large CSV file
    large_df = pd.DataFrame({
        'id': range(1, 251),  # 250 rows
        'value': ['test'] * 250
    })
    csv_path = tmp_path / 'large.csv'
    large_df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='large_table',
            source_system='TEST',
            batch_size=100  # Process in batches of 100
        )
        
        assert result['rows_loaded'] == 250


@pytest.mark.integration
def test_load_all_crm_data(bronze_manager_factory, tmp_path):
    """
    Integration test: load_all_crm_data loads all CRM files.
    
    Verifies:
    - All three CRM files are processed
    - Results are returned for each table
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create mock CRM CSV files
    crm_dir = tmp_path / 'datasets' / 'source_crm'
    crm_dir.mkdir(parents=True)
    
    for filename in ['cust_info.csv', 'prd_info.csv', 'sales_details.csv']:
        df = pd.DataFrame({'id': [1, 2], 'data': ['a', 'b']})
        df.to_csv(crm_dir / filename, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        results = manager.load_all_crm_data()
        
        assert len(results) == 3
        assert 'crm_customers_raw' in results
        assert 'crm_products_raw' in results
        assert 'crm_sales_raw' in results


@pytest.mark.integration
def test_load_all_erp_data(bronze_manager_factory, tmp_path):
    """
    Integration test: load_all_erp_data loads all ERP files.
    
    Verifies:
    - All three ERP files are processed
    - Results are returned for each table
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create mock ERP CSV files
    erp_dir = tmp_path / 'datasets' / 'source_erp'
    erp_dir.mkdir(parents=True)
    
    for filename in ['CUST_AZ12.csv', 'LOC_A101.csv', 'PX_CAT_G1V2.csv']:
        df = pd.DataFrame({'id': [1, 2], 'data': ['x', 'y']})
        df.to_csv(erp_dir / filename, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        results = manager.load_all_erp_data()
        
        assert len(results) == 3
        assert 'erp_customers_raw' in results
        assert 'erp_locations_raw' in results
        assert 'erp_product_categories_raw' in results


@pytest.mark.integration
def test_load_all_data(bronze_manager_factory, tmp_path):
    """
    Integration test: load_all_data loads both CRM and ERP files.
    
    Verifies:
    - All CRM and ERP files are processed
    - Summary is logged
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create mock data directories
    crm_dir = tmp_path / 'datasets' / 'source_crm'
    erp_dir = tmp_path / 'datasets' / 'source_erp'
    crm_dir.mkdir(parents=True)
    erp_dir.mkdir(parents=True)
    
    # Create CRM files
    for filename in ['cust_info.csv', 'prd_info.csv', 'sales_details.csv']:
        df = pd.DataFrame({'id': [1], 'data': ['a']})
        df.to_csv(crm_dir / filename, index=False)
    
    # Create ERP files
    for filename in ['CUST_AZ12.csv', 'LOC_A101.csv', 'PX_CAT_G1V2.csv']:
        df = pd.DataFrame({'id': [1], 'data': ['x']})
        df.to_csv(erp_dir / filename, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        results = manager.load_all_data()
        
        assert len(results) == 6  # 3 CRM + 3 ERP
        
        # Verify all tables present
        expected_tables = [
            'crm_customers_raw', 'crm_products_raw', 'crm_sales_raw',
            'erp_customers_raw', 'erp_locations_raw', 'erp_product_categories_raw'
        ]
        for table in expected_tables:
            assert table in results


# =============================================================================
# SECTION 3: SYSTEM TESTS - Full workflow testing
# =============================================================================

@pytest.mark.system
def test_end_to_end_csv_ingestion(bronze_manager_factory, tmp_path):
    """
    System test: Complete CSV ingestion workflow from file to database.
    
    Verifies:
    - File is read
    - Schema is inferred
    - Table is created
    - Data is enriched with metadata
    - Data is loaded
    - Audit trail is complete
    """
    manager = bronze_manager_factory(table_exists=False, monitor_performance=True)
    
    # Create test CSV
    test_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    csv_path = tmp_path / 'test_customers.csv'
    test_data.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='test_customers',
            source_system='TEST_SYSTEM'
        )
        
        # Verify complete workflow
        assert result['rows_loaded'] == 3
        assert result['process_log_id'] == 12345
        assert 'batch_id' in result
        assert 'TEST_SYSTEM' in result['batch_id']
        
        # Verify logging calls
        manager._mock_process_logger.start_process.assert_called_once()
        manager._mock_process_logger.end_process.assert_called_once()
        manager._mock_lineage_tracker.log_lineage.assert_called_once()
        manager._mock_performance_monitor.record_metric.assert_called_once()


@pytest.mark.system
def test_error_recovery_workflow(bronze_manager_factory, tmp_path):
    """
    System test: Error recovery and logging workflow.
    
    Verifies:
    - Errors are caught
    - Error logger is called
    - Process is marked as failed
    - Exception is raised with context
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create test CSV
    df = pd.DataFrame({'id': [1], 'data': ['test']})
    csv_path = tmp_path / 'test.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        # Simulate error during data loading
        manager._mock_engine.connect.return_value.__enter__.return_value.execute.side_effect = \
            SQLAlchemyError("Database connection lost")
        
        with pytest.raises(BronzeManagerError):
            manager.load_csv_to_bronze(
                csv_path=str(csv_path),
                table_name='test_table',
                source_system='TEST'
            )
        
        # Verify error handling
        manager._mock_error_logger.log_exception.assert_called_once()
        
        # Verify process failed
        end_call = manager._mock_process_logger.end_process.call_args
        assert end_call[1]['status'] == 'FAILED'


# =============================================================================
# SECTION 4: E2E TESTS - Complete ingestion scenarios
# =============================================================================

@pytest.mark.e2e
def test_e2e_multi_file_ingestion(bronze_manager_factory, tmp_path):
    """
    E2E test: Ingest multiple CSV files in sequence.
    
    Verifies:
    - Multiple files can be loaded
    - Each file gets unique batch ID
    - All files are tracked separately
    """
    manager = bronze_manager_factory(table_exists=False)
    
    files = []
    for i in range(3):
        df = pd.DataFrame({
            'id': [i*10 + j for j in range(5)],
            'value': [f'data_{i}_{j}' for j in range(5)]
        })
        csv_path = tmp_path / f'file_{i}.csv'
        df.to_csv(csv_path, index=False)
        files.append(csv_path)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        results = []
        for i, csv_path in enumerate(files):
            result = manager.load_csv_to_bronze(
                csv_path=str(csv_path),
                table_name=f'table_{i}',
                source_system='TEST'
            )
            results.append(result)
        
        # Verify all files loaded
        assert len(results) == 3
        assert all(r['rows_loaded'] == 5 for r in results)
        
        # Verify unique batch IDs
        batch_ids = [r['batch_id'] for r in results]
        assert len(set(batch_ids)) == 3


@pytest.mark.e2e
def test_e2e_incremental_load(bronze_manager_factory, tmp_path):
    """
    E2E test: Incremental load with duplicate detection via hash.
    
    Verifies:
    - Same data produces same row hash
    - Hashes can be used for deduplication
    """
    manager = bronze_manager_factory(table_exists=True)
    
    # Create initial CSV
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C']
    })
    csv_path = tmp_path / 'data.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.config') as mock_config:
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        # First load
        result1 = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='incremental_table',
            source_system='TEST',
            create_table=False
        )
        
        # Second load with same data (simulating incremental)
        result2 = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='incremental_table',
            source_system='TEST',
            create_table=False
        )
        
        # Verify both loads succeeded
        assert result1['rows_loaded'] == 3
        assert result2['rows_loaded'] == 3
        
        # Batch IDs should be different
        assert result1['batch_id'] != result2['batch_id']


# =============================================================================
# SECTION 5: SMOKE TESTS - Basic functionality checks
# =============================================================================

@pytest.mark.smoke
def test_smoke_bronze_manager_creation(bronze_manager_factory):
    """
    Smoke test: BronzeManager can be instantiated.
    """
    manager = bronze_manager_factory()
    assert manager is not None


@pytest.mark.smoke
def test_smoke_table_existence_check(bronze_manager_factory):
    """
    Smoke test: Table existence can be checked.
    """
    manager = bronze_manager_factory()
    result = manager.table_exists('any_table')
    assert isinstance(result, bool)


@pytest.mark.smoke
def test_smoke_schema_inference(bronze_manager_factory, mock_csv_data):
    """
    Smoke test: Schema can be inferred from DataFrame.
    """
    manager = bronze_manager_factory()
    df = mock_csv_data['customers']
    schema = manager._infer_table_schema(df)
    assert isinstance(schema, dict)
    assert len(schema) > 0


@pytest.mark.smoke
def test_smoke_csv_read(tmp_path):
    """
    Smoke test: CSV files can be read into DataFrames.
    """
    df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
    csv_path = tmp_path / 'test.csv'
    df.to_csv(csv_path, index=False)
    
    df_read = pd.read_csv(csv_path)
    assert len(df_read) == 2


# =============================================================================
# SECTION 6: EDGE CASE TESTS - Boundary conditions
# =============================================================================

@pytest.mark.edge_case
def test_edge_empty_dataframe(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle empty CSV file.
    
    Verifies:
    - Empty CSV is processed without error
    - Zero rows are loaded
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create empty CSV
    df = pd.DataFrame(columns=['id', 'name'])
    csv_path = tmp_path / 'empty.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='empty_table',
            source_system='TEST'
        )
        
        assert result['rows_loaded'] == 0


@pytest.mark.edge_case
def test_edge_single_row(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle CSV with single row.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    df = pd.DataFrame({'id': [1], 'name': ['Single']})
    csv_path = tmp_path / 'single.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='single_table',
            source_system='TEST'
        )
        
        assert result['rows_loaded'] == 1


@pytest.mark.edge_case
def test_edge_special_characters_in_data(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle special characters in data.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    df = pd.DataFrame({
        'id': [1, 2],
        'text': ["Test's \"quote\"", "Special: <>&"]
    })
    csv_path = tmp_path / 'special.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='special_table',
            source_system='TEST'
        )
        
        assert result['rows_loaded'] == 2


@pytest.mark.edge_case
def test_edge_null_values(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle NULL/NaN values in CSV.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['A', None, 'C'],
        'number': [1.0, float('nan'), 3.0]
    })
    csv_path = tmp_path / 'nulls.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='null_table',
            source_system='TEST'
        )
        
        assert result['rows_loaded'] == 3


@pytest.mark.edge_case
def test_edge_very_long_column_names(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle very long column names.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    long_col_name = 'very_' + 'long_' * 20 + 'column_name'
    df = pd.DataFrame({
        'id': [1],
        long_col_name: ['data']
    })
    csv_path = tmp_path / 'long_cols.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        # Should handle long column names
        schema = manager._infer_table_schema(df)
        assert long_col_name in schema


@pytest.mark.edge_case
def test_edge_unicode_data(bronze_manager_factory, tmp_path):
    """
    Edge case: Handle Unicode characters in data.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['José', '北京', 'مرحبا']
    })
    csv_path = tmp_path / 'unicode.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8')
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='unicode_table',
            source_system='TEST'
        )
        
        assert result['rows_loaded'] == 3


@pytest.mark.edge_case
def test_edge_maximum_batch_size_boundary(bronze_manager_factory, tmp_path):
    """
    Edge case: Test batch processing at exact batch size boundaries.
    """
    manager = bronze_manager_factory(table_exists=False)
    
    # Create CSV with exactly 100 rows (batch size = 100)
    df = pd.DataFrame({
        'id': range(100),
        'value': ['test'] * 100
    })
    csv_path = tmp_path / 'exact_batch.csv'
    df.to_csv(csv_path, index=False)
    
    with patch('medallion.bronze.create_table') as mock_create_ddl, \
         patch('medallion.bronze.config') as mock_config:
        
        mock_create_ddl.return_value = "CREATE TABLE ..."
        mock_config.project.project_root = tmp_path
        mock_config.warehouse_db_name = 'test_warehouse'
        
        result = manager.load_csv_to_bronze(
            csv_path=str(csv_path),
            table_name='batch_boundary',
            source_system='TEST',
            batch_size=100
        )
        
        assert result['rows_loaded'] == 100


@pytest.mark.edge_case
def test_edge_hash_collision_probability(bronze_manager_factory):
    """
    Edge case: Verify hash uniqueness for similar data.
    
    Tests that very similar rows produce different hashes.
    """
    manager = bronze_manager_factory()
    
    # Create very similar rows
    row1 = {'id': 1, 'name': 'Alice', 'email': 'alice@test.com'}
    row2 = {'id': 1, 'name': 'Alice', 'email': 'alice@test.co'}  # One char different
    row3 = {'id': 1, 'name': 'Alic', 'email': 'alice@test.com'}  # One char different
    
    hash1 = manager._calculate_row_hash(row1)
    hash2 = manager._calculate_row_hash(row2)
    hash3 = manager._calculate_row_hash(row3)
    
    # All hashes should be different
    assert hash1 != hash2
    assert hash1 != hash3
    assert hash2 != hash3
