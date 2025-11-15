"""
Shared fixtures and mocking helpers for medallion layer tests.

Key fixtures:
- patch_bronze_dependencies: patches BronzeManager dependencies
- mock_engine_factory: creates mock SQLAlchemy engines
- mock_csv_data: provides sample CSV data for testing
"""

import io
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_engine_factory():
    """
    Factory to create mock SQLAlchemy engines with configurable behavior.
    
    Returns:
        function: Factory function that creates mock engines
        
    Example:
        >>> engine = mock_engine_factory(table_exists=True)
        >>> with engine.connect() as conn:
        ...     result = conn.execute(text("SELECT 1"))
    """
    def factory(table_exists=False, execute_results=None, execute_side_effect=None):
        """
        Create a mock SQLAlchemy engine.
        
        Args:
            table_exists: Return value for table existence check
            execute_results: Dict mapping SQL keywords to result values
            execute_side_effect: Exception to raise on execute
            
        Returns:
            Mock engine object
        """
        execute_results = execute_results or {}
        
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [table_exists]
        mock_result.scalar.return_value = table_exists  # For EXISTS queries
        
        # Allow custom results based on SQL content
        def custom_execute(sql, *args, **kwargs):
            sql_str = str(sql).upper()
            for key, value in execute_results.items():
                if key.upper() in sql_str:
                    result = MagicMock()
                    result.fetchone.return_value = value
                    result.scalar.return_value = value[0] if isinstance(value, (list, tuple)) else value
                    return result
            if execute_side_effect:
                raise execute_side_effect
            return mock_result
        
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = custom_execute
        mock_conn.commit = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = False
        
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_engine.dispose = MagicMock()
        
        return mock_engine
    
    return factory


@pytest.fixture
def mock_csv_data():
    """
    Provides sample CSV data as pandas DataFrames for testing.
    
    Returns:
        dict: Dictionary of sample DataFrames for different data types
        
    Example:
        >>> csv_data = mock_csv_data
        >>> df = csv_data['customers']
        >>> assert len(df) > 0
    """
    return {
        'customers': pd.DataFrame({
            'customer_id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com'],
            'created_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'])
        }),
        'products': pd.DataFrame({
            'product_id': [101, 102, 103],
            'name': ['Widget', 'Gadget', 'Doohickey'],
            'price': [19.99, 29.99, 39.99],
            'stock': [100, 50, 25]
        }),
        'sales': pd.DataFrame({
            'sale_id': [1001, 1002, 1003],
            'customer_id': [1, 2, 1],
            'product_id': [101, 102, 103],
            'quantity': [2, 1, 3],
            'total': [39.98, 29.99, 119.97]
        }),
        'empty': pd.DataFrame()
    }


@pytest.fixture
def mock_csv_file(tmp_path, mock_csv_data):
    """
    Creates temporary CSV files for testing.
    
    Args:
        tmp_path: pytest fixture providing temporary directory
        mock_csv_data: fixture providing sample DataFrames
        
    Returns:
        dict: Dictionary mapping data type to CSV file path
        
    Example:
        >>> csv_files = mock_csv_file
        >>> assert csv_files['customers'].exists()
    """
    files = {}
    for name, df in mock_csv_data.items():
        if not df.empty:
            file_path = tmp_path / f"{name}.csv"
            df.to_csv(file_path, index=False)
            files[name] = file_path
    return files


@pytest.fixture
def mock_process_logger():
    """
    Mock ProcessLogger for testing audit trail functionality.
    
    Returns:
        Mock: Mock ProcessLogger instance
    """
    mock = MagicMock()
    mock.start_process.return_value = 12345  # Mock process log ID
    mock.end_process = MagicMock()
    return mock


@pytest.fixture
def mock_error_logger():
    """
    Mock ErrorLogger for testing error handling.
    
    Returns:
        Mock: Mock ErrorLogger instance
    """
    mock = MagicMock()
    mock.log_exception = MagicMock()
    mock.log_error = MagicMock()
    return mock


@pytest.fixture
def mock_lineage_tracker():
    """
    Mock LineageTracker for testing data lineage tracking.
    
    Returns:
        Mock: Mock LineageTracker instance
    """
    mock = MagicMock()
    mock.log_lineage = MagicMock()
    return mock


@pytest.fixture
def mock_performance_monitor():
    """
    Mock PerformanceMonitor for testing performance tracking.
    
    Returns:
        Mock: Mock PerformanceMonitor instance
    """
    mock = MagicMock()
    mock.record_metric = MagicMock()
    mock.monitor_process = MagicMock()
    mock.monitor_process.return_value.__enter__ = MagicMock(return_value=mock)
    mock.monitor_process.return_value.__exit__ = MagicMock(return_value=False)
    return mock


@pytest.fixture
def bronze_manager_factory(
    mock_engine_factory,
    mock_process_logger,
    mock_error_logger,
    mock_lineage_tracker,
    mock_performance_monitor
):
    """
    Factory that creates a BronzeManager with mocked dependencies.
    
    Returns:
        function: Factory function that creates BronzeManager instances
        
    Example:
        >>> manager = bronze_manager_factory(monitor_performance=True)
        >>> result = manager.load_csv_to_bronze(csv_path, table_name)
    """
    def factory(
        monitor_performance=True,
        bronze_schema='bronze',
        table_exists=False,
        db_exists=True,
        execute_results=None,
        execute_side_effect=None
    ):
        """
        Create a BronzeManager with mocked dependencies.
        
        Args:
            monitor_performance: Enable performance monitoring
            bronze_schema: Schema name
            table_exists: Mock table existence check result
            db_exists: Mock database existence check result
            execute_results: Custom execute results
            execute_side_effect: Exception to raise on execute
            
        Returns:
            BronzeManager: Configured instance with mocked dependencies
        """
        with patch('medallion.bronze.verify_database_exists') as mock_verify, \
             patch('medallion.bronze.create_sqlalchemy_engine') as mock_create_engine, \
             patch('medallion.bronze.ProcessLogger') as mock_pl_class, \
             patch('medallion.bronze.ErrorLogger') as mock_el_class, \
             patch('medallion.bronze.LineageTracker') as mock_lt_class, \
             patch('medallion.bronze.PerformanceMonitor') as mock_pm_class, \
             patch('medallion.bronze.ErrorRecovery') as mock_er_class:
            
            # Setup mocks
            mock_verify.return_value = db_exists
            mock_engine = mock_engine_factory(
                table_exists=table_exists,
                execute_results=execute_results,
                execute_side_effect=execute_side_effect
            )
            mock_create_engine.return_value = mock_engine
            
            # Setup logging infrastructure mocks
            mock_pl_class.return_value = mock_process_logger
            mock_el_class.return_value = mock_error_logger
            mock_lt_class.return_value = mock_lineage_tracker
            mock_pm_class.return_value = mock_performance_monitor
            mock_er_class.return_value = MagicMock()
            
            from medallion.bronze import BronzeManager
            
            manager = BronzeManager(
                monitor_performance=monitor_performance,
                bronze_schema=bronze_schema
            )
            
            # Store mocks for test assertions
            manager._mock_engine = mock_engine
            manager._mock_process_logger = mock_process_logger
            manager._mock_error_logger = mock_error_logger
            manager._mock_lineage_tracker = mock_lineage_tracker
            manager._mock_performance_monitor = mock_performance_monitor
            
            return manager
    
    return factory


@pytest.fixture
def patch_bronze_dependencies():
    """
    Context manager fixture to patch all BronzeManager dependencies.
    
    Yields:
        dict: Dictionary of patched objects
        
    Example:
        >>> with patch_bronze_dependencies() as mocks:
        ...     manager = BronzeManager()
        ...     assert mocks['verify_db'].called
    """
    with patch('medallion.bronze.verify_database_exists') as mock_verify, \
         patch('medallion.bronze.create_sqlalchemy_engine') as mock_engine, \
         patch('medallion.bronze.ProcessLogger') as mock_pl, \
         patch('medallion.bronze.ErrorLogger') as mock_el, \
         patch('medallion.bronze.LineageTracker') as mock_lt, \
         patch('medallion.bronze.PerformanceMonitor') as mock_pm, \
         patch('medallion.bronze.ErrorRecovery') as mock_er:
        
        yield {
            'verify_db': mock_verify,
            'create_engine': mock_engine,
            'process_logger': mock_pl,
            'error_logger': mock_el,
            'lineage_tracker': mock_lt,
            'performance_monitor': mock_pm,
            'error_recovery': mock_er
        }
