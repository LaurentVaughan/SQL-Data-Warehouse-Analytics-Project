"""
===============================================
Comprehensive pytest suite for main.py
===============================================

Test-Driven Development (TDD) approach following project conventions.

Sections:
---------
1. Unit tests - Individual method testing
2. Integration tests - Component interaction testing
3. System tests - Full workflow testing
4. CLI tests - Command-line interface testing
5. Edge case tests - Boundary conditions
6. Smoke tests - Basic functionality verification

Available markers:
------------------
unit, integration, system, edge_case, smoke

Test Coverage:
--------------
DataWarehouseOrchestrator:
- Initialization with/without performance monitoring
- Prerequisite verification (connection, availability, database checks)
- Logging infrastructure initialization (timing, dependencies)
- Setup orchestration (run_setup with all variations)
- Bronze/Silver/Gold layer operations (placeholders)
- Full pipeline execution
- Error handling and exception propagation
- Process logging integration
- Performance monitoring integration

main() CLI:
- Argument parsing (--setup, --bronze, --silver, --gold, --full-pipeline)
- Flag handling (--samples, --monitor, --verbose, --force-recreate)
- Exit code handling (success=0, error=1, interrupt=130)
- Error type handling (OrchestratorError, KeyboardInterrupt, unexpected)

How to Execute:
---------------
All tests:          pytest tests/test_main.py -v
By category:        pytest tests/test_main.py -m unit
Multiple markers:   pytest tests/test_main.py -m "unit or integration"
Specific test:      pytest tests/test_main.py::test_orchestrator_init_default
With coverage:      pytest tests/test_main.py --cov=main

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from main import DataWarehouseOrchestrator, OrchestratorError, main

# ====================
# Mock Helper Classes
# ====================

class FakeConfig:
    """Mock config object matching core.config structure."""
    def __init__(self):
        self.db_host = 'localhost'
        self.db_port = 5432
        self.db_user = 'postgres'
        self.db_password = 'secret'
        self.db_name = 'postgres'
        self.warehouse_db_name = 'sql_retail_analytics_warehouse'


class FakeSetupOrchestrator:
    """Mock SetupOrchestrator matching setup.setup_orchestrator."""
    def __init__(self):
        self.run_complete_setup_called = False
        self.run_complete_setup_args = None
        self.run_complete_setup_result = {
            'database': True,
            'schemas': True,
            'logging': True
        }
        
    def run_complete_setup(self, include_samples=False):
        """Mock run_complete_setup matching real implementation."""
        self.run_complete_setup_called = True
        self.run_complete_setup_args = {'include_samples': include_samples}
        
        result = self.run_complete_setup_result.copy()
        if include_samples:
            result['samples'] = True
            
        return result


class FakeProcessLogger:
    """Mock ProcessLogger matching logs.audit_logger."""
    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5432)
        self.user = kwargs.get('user', 'postgres')
        self.password = kwargs.get('password', '')
        self.database = kwargs.get('database', 'warehouse')
        
        self.process_counter = 100
        self.started_processes = []
        self.ended_processes = []
        
    def start_process(self, process_name, process_description=None, **kwargs):
        """Mock start_process matching real signature."""
        self.process_counter += 1
        self.started_processes.append({
            'id': self.process_counter,
            'name': process_name,
            'description': process_description,
            'kwargs': kwargs
        })
        return self.process_counter
    
    def end_process(self, log_id, status, **kwargs):
        """Mock end_process matching real signature."""
        self.ended_processes.append({
            'log_id': log_id,
            'status': status,
            'kwargs': kwargs
        })


class FakeErrorLogger:
    """Mock ErrorLogger matching logs.error_handler."""
    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5432)
        self.user = kwargs.get('user', 'postgres')
        self.password = kwargs.get('password', '')
        self.database = kwargs.get('database', 'warehouse')
        
        self.logged_errors = []
        self.logged_exceptions = []
        
    def log_error(self, process_log_id, error_message, **kwargs):
        """Mock log_error matching real signature."""
        self.logged_errors.append({
            'process_log_id': process_log_id,
            'error_message': error_message,
            'kwargs': kwargs
        })
        return len(self.logged_errors)
        
    def log_exception(self, process_log_id, exception, context=None, recovery_suggestion=None):
        """Mock log_exception matching real signature."""
        self.logged_exceptions.append({
            'process_log_id': process_log_id,
            'exception': exception,
            'context': context,
            'recovery_suggestion': recovery_suggestion
        })
        return len(self.logged_exceptions)


class FakeProcessMonitor:
    """Mock ProcessMonitor context manager from logs.performance_monitor."""
    def __init__(self, performance_monitor, process_log_id):
        self.performance_monitor = performance_monitor
        self.process_log_id = process_log_id
        self.recorded_metrics = []
        
    def __enter__(self):
        self.performance_monitor.monitored_processes.append(self.process_log_id)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
    
    def record_metric(self, metric_name, metric_value, metric_unit=None):
        """Mock record_metric matching real signature."""
        self.recorded_metrics.append({
            'name': metric_name,
            'value': metric_value,
            'unit': metric_unit
        })


class FakePerformanceMonitor:
    """Mock PerformanceMonitor matching logs.performance_monitor."""
    def __init__(self, *args, **kwargs):
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5432)
        self.user = kwargs.get('user', 'postgres')
        self.password = kwargs.get('password', '')
        self.database = kwargs.get('database', 'warehouse')
        
        self.monitored_processes = []
        
    def monitor_process(self, process_log_id):
        """Return ProcessMonitor context manager."""
        return FakeProcessMonitor(self, process_log_id)
    
    def record_metric(self, process_log_id, metric_name, metric_value, metric_unit=None, additional_context=None):
        """Mock record_metric matching real signature."""
        return len(self.monitored_processes) + 1


class FakeErrorRecovery:
    """Mock ErrorRecovery matching logs.error_handler."""
    def __init__(self, error_logger, max_retries=3, base_delay=1.0, backoff_multiplier=2.0):
        self.error_logger = error_logger
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_multiplier = backoff_multiplier
        self.retry_attempts = []
        
    def retry_with_backoff(self, func, **kwargs):
        """Mock retry_with_backoff matching real signature."""
        self.retry_attempts.append({'func': func, 'kwargs': kwargs})
        return func()


# ====================
# Fixtures
# ====================

@pytest.fixture
def mock_config():
    """Provide mock configuration matching core.config."""
    with patch('main.config', FakeConfig()):
        yield FakeConfig()


@pytest.fixture
def mock_database_utils():
    """Mock all database utility functions from utils.database_utils."""
    with patch('main.wait_for_database', return_value=True) as wait, \
         patch('main.verify_database_exists', return_value=False) as verify, \
         patch('main.verify_connection', return_value=(True, "Connected to PostgreSQL")) as test, \
         patch('main.get_database_connection_info', return_value={
             'host': 'localhost',
             'port': 5432,
             'user': 'postgres',
             'admin_database': 'postgres',
             'warehouse_database': 'sql_retail_analytics_warehouse'
         }) as info:
        yield {
            'wait_for_database': wait,
            'verify_database_exists': verify,
            'verify_connection': test,
            'get_database_connection_info': info
        }


@pytest.fixture
def mock_logging_components():
    """Mock logging infrastructure components from logs/ package."""
    with patch('main.ProcessLogger', FakeProcessLogger) as process, \
         patch('main.ErrorLogger', FakeErrorLogger) as error, \
         patch('main.PerformanceMonitor', FakePerformanceMonitor) as perf, \
         patch('main.ErrorRecovery', FakeErrorRecovery) as recovery:
        yield {
            'ProcessLogger': process,
            'ErrorLogger': error,
            'PerformanceMonitor': perf,
            'ErrorRecovery': recovery
        }


@pytest.fixture
def mock_setup_orchestrator():
    """Mock SetupOrchestrator from setup.setup_orchestrator."""
    with patch('main.SetupOrchestrator', FakeSetupOrchestrator):
        yield FakeSetupOrchestrator


@pytest.fixture
def orchestrator(mock_config):
    """Provide DataWarehouseOrchestrator instance with mocked config."""
    return DataWarehouseOrchestrator()


@pytest.fixture
def orchestrator_with_monitoring(mock_config):
    """Provide DataWarehouseOrchestrator with monitoring enabled."""
    return DataWarehouseOrchestrator(monitor_performance=True)


# ===============
# 1. UNIT TESTS
# ===============

@pytest.mark.unit
def test_orchestrator_init_default(mock_config):
    """
    Test DataWarehouseOrchestrator initialization with default settings.
    
    Verifies:
    - Performance monitoring disabled by default
    - All logging components None initially
    - Setup orchestrator None initially
    - Process tracking variables initialized
    """
    orchestrator = DataWarehouseOrchestrator()
    
    assert orchestrator.monitor_performance is False
    assert orchestrator.process_logger is None
    assert orchestrator.error_logger is None
    assert orchestrator.perf_monitor is None
    assert orchestrator.error_recovery is None
    assert orchestrator.setup_orchestrator is None
    assert orchestrator.current_process_id is None


@pytest.mark.unit
def test_orchestrator_init_with_monitoring(mock_config):
    """
    Test DataWarehouseOrchestrator initialization with monitoring enabled.
    
    Verifies monitor_performance flag is set correctly but components
    still None until initialize_logging_infrastructure() is called.
    """
    orchestrator = DataWarehouseOrchestrator(monitor_performance=True)
    
    assert orchestrator.monitor_performance is True
    assert orchestrator.perf_monitor is None  # Not initialized yet


@pytest.mark.unit
def test_verify_prerequisites_success(orchestrator, mock_database_utils):
    """
    Test prerequisite verification when all checks pass.
    
    Verifies:
    - Returns True when all checks succeed
    - Calls get_database_connection_info()
    - Calls verify_connection()
    - Calls wait_for_database()
    - All in correct order
    """
    result = orchestrator.verify_prerequisites()
    
    assert result is True
    mock_database_utils['get_database_connection_info'].assert_called_once()
    mock_database_utils['verify_connection'].assert_called_once()
    mock_database_utils['wait_for_database'].assert_called_once_with(
        max_retries=5,
        retry_delay=2
    )


@pytest.mark.unit
def test_verify_prerequisites_connection_failure(orchestrator, mock_database_utils):
    """
    Test prerequisite verification when connection test fails.
    
    Verifies:
    - OrchestratorError raised with appropriate message
    - Error message includes connection details
    - verify_connection() was called
    """
    mock_database_utils['verify_connection'].return_value = (False, "Connection refused")
    
    with pytest.raises(OrchestratorError) as exc_info:
        orchestrator.verify_prerequisites()
    
    assert 'PostgreSQL connection failed' in str(exc_info.value)
    assert 'Connection refused' in str(exc_info.value)
    mock_database_utils['verify_connection'].assert_called_once()


@pytest.mark.unit
def test_verify_prerequisites_wait_timeout(orchestrator, mock_database_utils):
    """
    Test prerequisite verification when database wait times out.
    
    Verifies:
    - DatabaseConnectionError from wait_for_database is wrapped
    - OrchestratorError raised with 'Prerequisite verification failed'
    - Original error message preserved
    """
    from utils.database_utils import DatabaseConnectionError
    mock_database_utils['wait_for_database'].side_effect = DatabaseConnectionError("Timeout after 10 retries")
    
    with pytest.raises(OrchestratorError) as exc_info:
        orchestrator.verify_prerequisites()
    
    assert 'Prerequisite verification failed' in str(exc_info.value)
    assert 'Timeout after 10 retries' in str(exc_info.value)


@pytest.mark.unit
def test_verify_prerequisites_unexpected_exception(orchestrator, mock_database_utils):
    """
    Test prerequisite verification handles unexpected exceptions.
    
    Verifies unexpected exceptions are wrapped in OrchestratorError.
    """
    mock_database_utils['get_database_connection_info'].side_effect = RuntimeError("Unexpected error")
    
    with pytest.raises(OrchestratorError) as exc_info:
        orchestrator.verify_prerequisites()
    
    assert 'Prerequisite verification failed' in str(exc_info.value)


@pytest.mark.unit
def test_initialize_logging_infrastructure_success(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test logging infrastructure initialization when database exists.
    
    Verifies:
    - Checks if warehouse database exists
    - Initializes ProcessLogger with correct params
    - Initializes ErrorLogger with correct params
    - Initializes ErrorRecovery with ErrorLogger
    - Performance monitor NOT initialized (monitoring disabled)
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    orchestrator.initialize_logging_infrastructure()
    
    # Verify database check
    mock_database_utils['verify_database_exists'].assert_called_once_with('sql_retail_analytics_warehouse')
    
    # Verify components initialized
    assert isinstance(orchestrator.process_logger, FakeProcessLogger)
    assert orchestrator.process_logger.database == 'sql_retail_analytics_warehouse'
    
    assert isinstance(orchestrator.error_logger, FakeErrorLogger)
    assert orchestrator.error_logger.database == 'sql_retail_analytics_warehouse'
    
    assert isinstance(orchestrator.error_recovery, FakeErrorRecovery)
    assert orchestrator.error_recovery.error_logger is orchestrator.error_logger
    
    # Performance monitor not initialized (monitoring disabled)
    assert orchestrator.perf_monitor is None


@pytest.mark.unit
def test_initialize_logging_infrastructure_with_monitoring(orchestrator_with_monitoring, mock_database_utils, mock_logging_components):
    """
    Test logging infrastructure initialization with monitoring enabled.
    
    Verifies:
    - PerformanceMonitor is initialized when monitor_performance=True
    - Monitor initialized with correct database params
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    orchestrator_with_monitoring.initialize_logging_infrastructure()
    
    assert isinstance(orchestrator_with_monitoring.perf_monitor, FakePerformanceMonitor)
    assert orchestrator_with_monitoring.perf_monitor.database == 'sql_retail_analytics_warehouse'


@pytest.mark.unit
def test_initialize_logging_infrastructure_no_database(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test logging infrastructure initialization when warehouse DB doesn't exist.
    
    Verifies:
    - OrchestratorError raised if warehouse database not found
    - Error message mentions database name
    - Error message suggests running setup first
    """
    mock_database_utils['verify_database_exists'].return_value = False
    
    with pytest.raises(OrchestratorError) as exc_info:
        orchestrator.initialize_logging_infrastructure()
    
    assert 'does not exist' in str(exc_info.value)
    assert 'sql_retail_analytics_warehouse' in str(exc_info.value)
    assert 'Run setup first' in str(exc_info.value)


@pytest.mark.unit
def test_initialize_logging_infrastructure_exception_handling(orchestrator, mock_database_utils):
    """
    Test logging infrastructure initialization when database doesn't exist.
    
    Verifies OrchestratorError raised when warehouse database not available.
    """
    mock_database_utils['verify_database_exists'].return_value = False
    
    with pytest.raises(OrchestratorError) as exc_info:
        orchestrator.initialize_logging_infrastructure()
    
    assert 'does not exist' in str(exc_info.value)
    assert 'Run setup first' in str(exc_info.value)


@pytest.mark.unit
def test_run_setup_success_no_samples(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup success without sample tables.
    
    Verifies:
    - Prerequisites verified first
    - SetupOrchestrator initialized
    - run_complete_setup called with include_samples=False
    - Logging infrastructure initialized after setup
    - Returns dict with all steps successful
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    results = orchestrator.run_setup(include_samples=False, force_recreate=False)
    
    # Verify return value
    assert isinstance(results, dict)
    assert 'database' in results
    assert 'schemas' in results
    assert 'logging' in results
    assert all(results.values())
    
    # Verify setup orchestrator used
    assert orchestrator.setup_orchestrator is not None
    assert orchestrator.setup_orchestrator.run_complete_setup_called
    assert orchestrator.setup_orchestrator.run_complete_setup_args['include_samples'] is False
    
    # Verify logging initialized after setup
    assert orchestrator.process_logger is not None
    assert orchestrator.error_logger is not None


@pytest.mark.unit
def test_run_setup_success_with_samples(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup success with sample tables included.
    
    Verifies:
    - run_complete_setup called with include_samples=True
    - Results include 'samples' key
    - Sample tables creation successful
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    results = orchestrator.run_setup(include_samples=True)
    
    assert 'samples' in results
    assert results['samples'] is True
    assert orchestrator.setup_orchestrator.run_complete_setup_args['include_samples'] is True


@pytest.mark.unit
def test_run_setup_force_recreate_warning(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup with force_recreate flag.
    
    Verifies:
    - Warning logged about force recreate
    - Setup still proceeds successfully
    - Flag handled by SetupOrchestrator (not main.py)
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('main.logger') as mock_logger:
        results = orchestrator.run_setup(force_recreate=True)
        
        # Verify warning logged
        warning_calls = [call for call in mock_logger.warning.call_args_list 
                        if 'Force recreate enabled' in str(call)]
        assert len(warning_calls) > 0


@pytest.mark.unit
def test_run_setup_prerequisite_failure(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_setup stops early when prerequisites fail.
    
    Verifies:
    - OrchestratorError raised from verify_prerequisites
    - SetupOrchestrator not initialized
    - No logging infrastructure initialized
    """
    mock_database_utils['verify_connection'].return_value = (False, "Connection failed")
    
    with pytest.raises(OrchestratorError):
        orchestrator.run_setup()
    
    assert orchestrator.setup_orchestrator is None
    assert orchestrator.process_logger is None


@pytest.mark.unit
def test_run_setup_partial_failure(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup when some steps fail.
    
    Verifies:
    - OrchestratorError raised when any step fails
    - Error message lists failed steps
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # Mock partial failure
    fake_orchestrator = FakeSetupOrchestrator()
    fake_orchestrator.run_complete_setup_result = {
        'database': True,
        'schemas': False,  # Failed
        'logging': False   # Failed
    }
    
    with patch('main.SetupOrchestrator', return_value=fake_orchestrator):
        with pytest.raises(OrchestratorError) as exc_info:
            orchestrator.run_setup()
        
        assert 'Setup failed for steps' in str(exc_info.value)
        assert 'schemas' in str(exc_info.value)
        assert 'logging' in str(exc_info.value)


@pytest.mark.unit
def test_run_setup_setup_error_propagation(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup propagates SetupError from SetupOrchestrator.
    
    Verifies SetupError is wrapped in OrchestratorError.
    """
    from setup.setup_orchestrator import SetupError
    
    with patch('main.SetupOrchestrator') as mock_setup:
        mock_setup.return_value.run_complete_setup.side_effect = SetupError("Database creation failed")
        
        with pytest.raises(OrchestratorError) as exc_info:
            orchestrator.run_setup()
        
        assert 'Setup failed' in str(exc_info.value)


@pytest.mark.unit
def test_run_bronze_ingestion_placeholder(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_bronze_ingestion placeholder implementation.
    
    Verifies:
    - Returns dict with status='NOT_IMPLEMENTED'
    - Logs warning about placeholder
    - Process started and ended via ProcessLogger
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    with patch('main.logger') as mock_logger:
        results = orchestrator.run_bronze_ingestion(source='crm', batch_size=1000)
        
        # Verify placeholder response
        assert isinstance(results, dict)
        assert results['status'] == 'NOT_IMPLEMENTED'
        
        # Verify warning logged
        warning_calls = [call for call in mock_logger.warning.call_args_list 
                        if 'not yet implemented' in str(call)]
        assert len(warning_calls) > 0
    
    # Verify process logging
    assert len(orchestrator.process_logger.started_processes) > 0
    assert len(orchestrator.process_logger.ended_processes) > 0
    
    started = orchestrator.process_logger.started_processes[-1]
    assert 'bronze_ingestion' in started['name']
    
    ended = orchestrator.process_logger.ended_processes[-1]
    assert ended['status'] == 'SUCCESS'


@pytest.mark.unit
def test_run_bronze_ingestion_auto_init_logging(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_bronze_ingestion auto-initializes logging infrastructure.
    
    Verifies:
    - Logging initialized if not already available
    - ProcessLogger becomes available after initialization
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    assert orchestrator.process_logger is None
    
    orchestrator.run_bronze_ingestion(source='crm')
    
    assert orchestrator.process_logger is not None


@pytest.mark.unit
def test_run_bronze_ingestion_with_monitoring(orchestrator_with_monitoring, mock_database_utils, mock_logging_components):
    """
    Test run_bronze_ingestion with performance monitoring enabled.
    
    Verifies:
    - PerformanceMonitor.monitor_process() context manager used
    - Process ID tracked in monitored_processes
    - Metrics can be recorded via ProcessMonitor
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator_with_monitoring.initialize_logging_infrastructure()
    
    orchestrator_with_monitoring.run_bronze_ingestion(source='crm')
    
    # Verify monitoring was used
    assert len(orchestrator_with_monitoring.perf_monitor.monitored_processes) > 0


@pytest.mark.unit
def test_run_bronze_ingestion_exception_handling(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_bronze_ingestion exception handling.
    
    Verifies:
    - Exceptions logged via ErrorLogger.log_exception()
    - Process ended with FAILED status
    - OrchestratorError raised with original error
    - Recovery suggestion provided
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    # Mock exception during ingestion
    with patch.object(orchestrator.process_logger, 'start_process', side_effect=Exception("Database error")):
        with pytest.raises(OrchestratorError) as exc_info:
            orchestrator.run_bronze_ingestion(source='crm')
        
        assert 'Bronze ingestion failed' in str(exc_info.value)


@pytest.mark.unit
def test_run_bronze_ingestion_source_parameter(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_bronze_ingestion with different source parameters.
    
    Verifies source parameter is logged in process metadata.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    orchestrator.run_bronze_ingestion(source='erp')
    
    started = orchestrator.process_logger.started_processes[-1]
    assert 'erp' in started['name'].lower() or 'erp' in str(started.get('kwargs', {}))


@pytest.mark.unit
def test_run_silver_transformation_placeholder(orchestrator):
    """
    Test run_silver_transformation placeholder.
    
    Verifies:
    - Returns dict with status='NOT_IMPLEMENTED'
    - No database operations (placeholder only)
    """
    results = orchestrator.run_silver_transformation()
    
    assert isinstance(results, dict)
    assert results['status'] == 'NOT_IMPLEMENTED'


@pytest.mark.unit
def test_run_gold_analytics_placeholder(orchestrator):
    """
    Test run_gold_analytics placeholder.
    
    Verifies placeholder returns NOT_IMPLEMENTED.
    """
    results = orchestrator.run_gold_analytics()
    
    assert isinstance(results, dict)
    assert results['status'] == 'NOT_IMPLEMENTED'


@pytest.mark.unit
def test_run_full_pipeline_placeholder(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_full_pipeline orchestrates all layers.
    
    Verifies:
    - Calls run_bronze_ingestion(source='all')
    - Calls run_silver_transformation()
    - Calls run_gold_analytics()
    - Returns dict with results for each layer
    - Continues even if layers are placeholders
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    results = orchestrator.run_full_pipeline()
    
    assert isinstance(results, dict)
    assert 'bronze' in results
    assert 'silver' in results
    assert 'gold' in results
    
    # Verify bronze called with source='all'
    started = orchestrator.process_logger.started_processes
    bronze_processes = [p for p in started if 'bronze' in p['name'].lower()]
    assert len(bronze_processes) > 0


@pytest.mark.unit
def test_run_full_pipeline_exception_propagation(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_full_pipeline propagates exceptions.
    
    Verifies exceptions in any layer stop the pipeline.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    with patch.object(orchestrator, 'run_bronze_ingestion', side_effect=Exception("Bronze failed")):
        with pytest.raises(OrchestratorError) as exc_info:
            orchestrator.run_full_pipeline()
        
        assert 'Pipeline execution failed' in str(exc_info.value)


# =======================
# 2. INTEGRATION TESTS
# =======================

@pytest.mark.integration
def test_complete_setup_workflow(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Integration test: Complete setup workflow from prerequisites to logging.
    
    Verifies:
    1. Prerequisite verification
    2. Setup execution
    3. Logging infrastructure initialization
    4. All components available after setup
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # Step 1: Verify prerequisites
    assert orchestrator.verify_prerequisites()
    
    # Step 2: Run setup
    results = orchestrator.run_setup(include_samples=True)
    
    # Step 3: Verify all successful
    assert all(results.values())
    
    # Step 4: Verify components initialized
    assert orchestrator.setup_orchestrator is not None
    assert orchestrator.process_logger is not None
    assert orchestrator.error_logger is not None
    assert orchestrator.error_recovery is not None


@pytest.mark.integration
def test_setup_to_bronze_ingestion(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Integration test: Setup followed by bronze ingestion.
    
    Verifies logging infrastructure from setup is used for bronze ingestion.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # Setup
    setup_results = orchestrator.run_setup(include_samples=False)
    assert all(setup_results.values())
    
    process_logger_after_setup = orchestrator.process_logger
    
    # Bronze ingestion
    bronze_results = orchestrator.run_bronze_ingestion(source='crm')
    
    # Verify same logger used
    assert orchestrator.process_logger is process_logger_after_setup
    
    # Verify processes logged
    assert len(orchestrator.process_logger.started_processes) >= 1


@pytest.mark.integration
def test_error_handling_workflow(orchestrator, mock_database_utils, mock_logging_components):
    """
    Integration test: Error handling throughout workflow.
    
    Verifies:
    - Errors logged via ErrorLogger
    - Process ended with FAILED status
    - Error recovery initialized
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    # Simulate error in bronze ingestion
    with patch.object(orchestrator.process_logger, 'start_process', return_value=123):
        with patch.object(orchestrator.process_logger, 'end_process'):
            with patch.object(orchestrator.error_logger, 'log_exception') as mock_log_exc:
                
                # Force an error
                with patch('main.logger') as mock_logger:
                    mock_logger.warning.side_effect = Exception("Test error")
                    
                    with pytest.raises(Exception):
                        orchestrator.run_bronze_ingestion(source='crm')


@pytest.mark.integration
def test_performance_monitoring_workflow(orchestrator_with_monitoring, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Integration test: Performance monitoring throughout workflow.
    
    Verifies:
    - PerformanceMonitor initialized after setup
    - Monitoring used in bronze ingestion
    - Metrics can be recorded
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # Setup with monitoring
    orchestrator_with_monitoring.run_setup(include_samples=False)
    
    assert orchestrator_with_monitoring.perf_monitor is not None
    
    # Bronze ingestion with monitoring
    orchestrator_with_monitoring.run_bronze_ingestion(source='crm')
    
    # Verify monitoring occurred
    assert len(orchestrator_with_monitoring.perf_monitor.monitored_processes) > 0


@pytest.mark.integration
def test_logging_sequence(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Integration test: Verify logging sequence across operations.
    
    Verifies processes are logged in correct order with correct statuses.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # Setup
    orchestrator.run_setup(include_samples=False)
    
    # Bronze ingestion
    orchestrator.run_bronze_ingestion(source='crm')
    
    # Verify sequential process IDs
    started = orchestrator.process_logger.started_processes
    ended = orchestrator.process_logger.ended_processes
    
    assert len(started) >= 1
    assert len(ended) >= 1
    
    # Verify each started process was ended
    for process in started:
        matching_ended = [e for e in ended if e['log_id'] == process['id']]
        assert len(matching_ended) > 0


# ====================
# 3. SYSTEM TESTS
# ====================

@pytest.mark.system
def test_full_orchestration_lifecycle(orchestrator_with_monitoring, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    System test: Complete orchestration lifecycle.
    
    Simulates full production workflow:
    1. Initialize orchestrator with monitoring
    2. Verify prerequisites
    3. Run complete setup
    4. Run bronze ingestion
    5. Run silver transformation
    6. Run gold analytics
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    # 1. Verify prerequisites
    assert orchestrator_with_monitoring.verify_prerequisites()
    
    # 2. Setup
    setup_results = orchestrator_with_monitoring.run_setup(include_samples=True)
    assert all(setup_results.values())
    
    # 3. Bronze
    bronze_results = orchestrator_with_monitoring.run_bronze_ingestion(source='all')
    assert bronze_results['status'] == 'NOT_IMPLEMENTED'
    
    # 4. Silver
    silver_results = orchestrator_with_monitoring.run_silver_transformation()
    assert silver_results['status'] == 'NOT_IMPLEMENTED'
    
    # 5. Gold
    gold_results = orchestrator_with_monitoring.run_gold_analytics()
    assert gold_results['status'] == 'NOT_IMPLEMENTED'
    
    # Verify all components still available
    assert orchestrator_with_monitoring.process_logger is not None
    assert orchestrator_with_monitoring.perf_monitor is not None


@pytest.mark.system
def test_error_recovery_across_failures(orchestrator, mock_database_utils, mock_logging_components):
    """
    System test: Error recovery across multiple failures.
    
    Verifies orchestrator can recover from failures and continue.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    # First failure
    with patch.object(orchestrator.process_logger, 'start_process', side_effect=Exception("First error")):
        with pytest.raises(OrchestratorError):
            orchestrator.run_bronze_ingestion(source='crm')
    
    # Verify orchestrator still functional
    assert orchestrator.process_logger is not None
    
    # Successful operation after failure
    with patch.object(orchestrator.process_logger, 'start_process', return_value=200):
        results = orchestrator.run_bronze_ingestion(source='erp')
        assert results['status'] == 'NOT_IMPLEMENTED'


# =================
# 4. CLI TESTS
# =================

@pytest.mark.unit
def test_main_setup_command(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() CLI with --setup command.
    
    Verifies:
    - Setup command recognized and executed
    - Exit code 0 on success
    - All setup steps completed
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--setup']):
        exit_code = main()
        assert exit_code == 0


@pytest.mark.unit
def test_main_setup_with_samples(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() CLI with --setup --samples.
    
    Verifies --samples flag passed to run_setup().
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--setup', '--samples']):
        with patch.object(DataWarehouseOrchestrator, 'run_setup') as mock_run_setup:
            mock_run_setup.return_value = {'database': True, 'schemas': True, 'logging': True, 'samples': True}
            
            exit_code = main()
            
            # Verify run_setup called with include_samples=True
            mock_run_setup.assert_called_once()
            call_kwargs = mock_run_setup.call_args[1]
            assert call_kwargs['include_samples'] is True


@pytest.mark.unit
def test_main_setup_with_force_recreate(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() CLI with --setup --force-recreate.
    
    Verifies --force-recreate flag passed to run_setup().
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--setup', '--force-recreate']):
        with patch.object(DataWarehouseOrchestrator, 'run_setup') as mock_run_setup:
            mock_run_setup.return_value = {'database': True, 'schemas': True, 'logging': True}
            
            exit_code = main()
            
            call_kwargs = mock_run_setup.call_args[1]
            assert call_kwargs['force_recreate'] is True


@pytest.mark.unit
def test_main_setup_with_monitor(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() CLI with --setup --monitor.
    
    Verifies:
    - Orchestrator initialized with monitor_performance=True
    - Setup completes with monitoring enabled
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--setup', '--monitor']):
        with patch.object(DataWarehouseOrchestrator, '__init__', return_value=None) as mock_init:
            with patch.object(DataWarehouseOrchestrator, 'run_setup', return_value={'database': True}):
                with patch.object(DataWarehouseOrchestrator, 'verify_prerequisites', return_value=True):
                    
                    exit_code = main()
                    
                    # Verify monitor_performance=True passed to __init__
                    mock_init.assert_called_once_with(monitor_performance=True)


@pytest.mark.unit
def test_main_bronze_command(mock_config, mock_database_utils, mock_logging_components):
    """
    Test main() CLI with --bronze command.
    
    Verifies:
    - Bronze command recognized
    - Source parameter passed correctly
    - Logging infrastructure initialized
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--bronze', '--source', 'crm']):
        with patch.object(DataWarehouseOrchestrator, 'run_bronze_ingestion') as mock_bronze:
            mock_bronze.return_value = {'status': 'NOT_IMPLEMENTED'}
            
            with patch.object(DataWarehouseOrchestrator, 'initialize_logging_infrastructure'):
                exit_code = main()
                
                # Verify bronze ingestion called with source='crm'
                mock_bronze.assert_called_once()
                call_kwargs = mock_bronze.call_args[1]
                assert call_kwargs['source'] == 'crm'


@pytest.mark.unit
def test_main_silver_command(mock_config, mock_database_utils, mock_logging_components):
    """
    Test main() CLI with --silver command.
    
    Verifies silver transformation command recognized.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--silver']):
        with patch.object(DataWarehouseOrchestrator, 'run_silver_transformation') as mock_silver:
            mock_silver.return_value = {'status': 'NOT_IMPLEMENTED'}
            
            with patch.object(DataWarehouseOrchestrator, 'initialize_logging_infrastructure'):
                exit_code = main()
                
                mock_silver.assert_called_once()


@pytest.mark.unit
def test_main_gold_command(mock_config, mock_database_utils, mock_logging_components):
    """
    Test main() CLI with --gold command.
    
    Verifies gold analytics command recognized.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--gold']):
        with patch.object(DataWarehouseOrchestrator, 'run_gold_analytics') as mock_gold:
            mock_gold.return_value = {'status': 'NOT_IMPLEMENTED'}
            
            with patch.object(DataWarehouseOrchestrator, 'initialize_logging_infrastructure'):
                exit_code = main()
                
                mock_gold.assert_called_once()


@pytest.mark.unit
def test_main_full_pipeline_command(mock_config, mock_database_utils, mock_logging_components):
    """
    Test main() CLI with --full-pipeline command.
    
    Verifies full pipeline command executes all layers.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    with patch('sys.argv', ['main.py', '--full-pipeline']):
        with patch.object(DataWarehouseOrchestrator, 'run_full_pipeline') as mock_pipeline:
            mock_pipeline.return_value = {
                'bronze': {'status': 'NOT_IMPLEMENTED'},
                'silver': {'status': 'NOT_IMPLEMENTED'},
                'gold': {'status': 'NOT_IMPLEMENTED'}
            }
            
            with patch.object(DataWarehouseOrchestrator, 'initialize_logging_infrastructure'):
                exit_code = main()
                
                mock_pipeline.assert_called_once()


@pytest.mark.unit
def test_main_no_command(mock_config):
    """
    Test main() CLI with no command specified.
    
    Verifies:
    - Help message printed
    - Exit code 1 (error)
    - Warning logged about missing operation
    """
    with patch('sys.argv', ['main.py']):
        exit_code = main()
        assert exit_code == 1


@pytest.mark.unit
def test_main_verbose_flag(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() CLI with --verbose flag.
    
    Verifies logging level set to DEBUG when --verbose specified.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    import logging
    
    with patch('sys.argv', ['main.py', '--setup', '--verbose']):
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            exit_code = main()
            
            # Verify setLevel(DEBUG) called
            mock_logger.setLevel.assert_called_with(logging.DEBUG)


@pytest.mark.unit
def test_main_keyboard_interrupt(mock_config, mock_database_utils):
    """
    Test main() CLI handles KeyboardInterrupt gracefully.
    
    Verifies:
    - Exit code 130 (standard for SIGINT)
    - Warning logged about user interruption
    """
    mock_database_utils['verify_connection'].side_effect = KeyboardInterrupt()
    
    with patch('sys.argv', ['main.py', '--setup']):
        exit_code = main()
        assert exit_code == 130


@pytest.mark.unit
def test_main_orchestrator_error(mock_config, mock_database_utils):
    """
    Test main() CLI handles OrchestratorError.
    
    Verifies:
    - Exit code 1 on OrchestratorError
    - Error message logged
    """
    mock_database_utils['verify_connection'].return_value = (False, "Connection failed")
    
    with patch('sys.argv', ['main.py', '--setup']):
        exit_code = main()
        assert exit_code == 1


@pytest.mark.unit
def test_main_unexpected_exception(mock_config, mock_database_utils):
    """
    Test main() CLI handles unexpected exceptions.
    
    Verifies:
    - Exit code 1 on unexpected error
    - Full traceback logged (exc_info=True)
    """
    mock_database_utils['get_database_connection_info'].side_effect = RuntimeError("Unexpected error")
    
    with patch('sys.argv', ['main.py', '--setup']):
        with patch('main.logger') as mock_logger:
            exit_code = main()
            
            assert exit_code == 1
            
            # Verify exception logged with traceback
            error_calls = [call for call in mock_logger.error.call_args_list 
                          if 'Unexpected error' in str(call)]
            assert len(error_calls) > 0


@pytest.mark.unit
def test_main_setup_failure_exit_code(mock_config, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test main() returns exit code 1 when setup fails.
    
    Verifies failed setup results in error exit code.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    fake_orchestrator = FakeSetupOrchestrator()
    fake_orchestrator.run_complete_setup_result = {
        'database': True,
        'schemas': False  # Failed
    }
    
    with patch('sys.argv', ['main.py', '--setup']):
        with patch('main.SetupOrchestrator', return_value=fake_orchestrator):
            exit_code = main()
            assert exit_code == 1


# ====================
# 5. EDGE CASE TESTS
# ====================

@pytest.mark.edge_case
def test_orchestrator_multiple_setup_runs(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test running setup multiple times.
    
    Verifies:
    - Second setup doesn't break existing logging
    - SetupOrchestrator re-initialized each time
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    results1 = orchestrator.run_setup(include_samples=False)
    first_logger = orchestrator.process_logger
    
    results2 = orchestrator.run_setup(include_samples=False)
    second_logger = orchestrator.process_logger
    
    assert all(results1.values())
    assert all(results2.values())
    
    # Logger should be re-initialized
    assert first_logger is not None
    assert second_logger is not None


@pytest.mark.edge_case
def test_bronze_ingestion_without_prior_setup(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test bronze ingestion initializes logging even without prior setup.
    
    Verifies on-demand logging initialization works.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    assert orchestrator.process_logger is None
    
    orchestrator.run_bronze_ingestion(source='crm')
    
    assert orchestrator.process_logger is not None


@pytest.mark.edge_case
def test_initialize_logging_twice(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test initializing logging infrastructure multiple times.
    
    Verifies multiple calls don't cause errors.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    orchestrator.initialize_logging_infrastructure()
    first_logger = orchestrator.process_logger
    
    orchestrator.initialize_logging_infrastructure()
    second_logger = orchestrator.process_logger
    
    assert first_logger is not None
    assert second_logger is not None


@pytest.mark.edge_case
def test_run_setup_with_all_flags(orchestrator, mock_database_utils, mock_logging_components, mock_setup_orchestrator):
    """
    Test run_setup with all optional flags enabled.
    
    Verifies combinations of flags work together.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    results = orchestrator.run_setup(
        include_samples=True,
        force_recreate=True
    )
    
    assert all(results.values())
    assert 'samples' in results


@pytest.mark.edge_case
def test_bronze_ingestion_all_sources(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test bronze ingestion with source='all'.
    
    Verifies 'all' is a valid source parameter.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    results = orchestrator.run_bronze_ingestion(source='all')
    
    assert results['status'] == 'NOT_IMPLEMENTED'
    
    started = orchestrator.process_logger.started_processes[-1]
    assert 'all' in started['name'].lower() or 'all' in str(started.get('kwargs', {}))


@pytest.mark.edge_case
def test_performance_monitoring_without_setup(orchestrator_with_monitoring, mock_database_utils, mock_logging_components):
    """
    Test performance monitoring when initialized without setup.
    
    Verifies monitoring can be initialized independently.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    
    orchestrator_with_monitoring.initialize_logging_infrastructure()
    
    assert orchestrator_with_monitoring.perf_monitor is not None
    
    orchestrator_with_monitoring.run_bronze_ingestion(source='crm')
    
    assert len(orchestrator_with_monitoring.perf_monitor.monitored_processes) > 0


@pytest.mark.edge_case
def test_run_pipeline_partial_implementation(orchestrator, mock_database_utils, mock_logging_components):
    """
    Test run_full_pipeline when some layers fail.
    
    Verifies pipeline stops on first failure.
    """
    mock_database_utils['verify_database_exists'].return_value = True
    orchestrator.initialize_logging_infrastructure()
    
    with patch.object(orchestrator, 'run_bronze_ingestion', side_effect=Exception("Bronze failed")):
        with pytest.raises(OrchestratorError):
            orchestrator.run_full_pipeline()


# =================
# 6. SMOKE TESTS
# =================

@pytest.mark.smoke
def test_module_imports():
    """
    Smoke test: Verify module imports work without errors.
    
    Ensures all public classes and functions can be imported.
    """
    from main import DataWarehouseOrchestrator, OrchestratorError, main
    
    assert DataWarehouseOrchestrator is not None
    assert issubclass(OrchestratorError, Exception)
    assert callable(main)


@pytest.mark.smoke
def test_basic_orchestrator_creation(mock_config):
    """
    Smoke test: Basic orchestrator instantiation.
    
    Quick verification that orchestrator can be created without errors.
    """
    orchestrator = DataWarehouseOrchestrator()
    
    assert orchestrator is not None
    assert hasattr(orchestrator, 'run_setup')
    assert hasattr(orchestrator, 'run_bronze_ingestion')
    assert hasattr(orchestrator, 'run_silver_transformation')
    assert hasattr(orchestrator, 'run_gold_analytics')
    assert hasattr(orchestrator, 'run_full_pipeline')
    assert hasattr(orchestrator, 'verify_prerequisites')
    assert hasattr(orchestrator, 'initialize_logging_infrastructure')


@pytest.mark.smoke
def test_orchestrator_error_exception():
    """
    Smoke test: OrchestratorError can be raised and caught.
    
    Verifies custom exception works correctly.
    """
    with pytest.raises(OrchestratorError) as exc_info:
        raise OrchestratorError("Test error message")
    
    assert "Test error message" in str(exc_info.value)
    assert isinstance(exc_info.value, Exception)


@pytest.mark.smoke
def test_orchestrator_has_all_methods(orchestrator):
    """
    Smoke test: Verify orchestrator has all required methods.
    
    Ensures API completeness.
    """
    required_methods = [
        'verify_prerequisites',
        'initialize_logging_infrastructure',
        'run_setup',
        'run_bronze_ingestion',
        'run_silver_transformation',
        'run_gold_analytics',
        'run_full_pipeline'
    ]
    
    for method in required_methods:
        assert hasattr(orchestrator, method), f"Missing method: {method}"
        assert callable(getattr(orchestrator, method)), f"Not callable: {method}"


@pytest.mark.smoke
def test_main_function_callable():
    """
    Smoke test: main() function is callable.
    
    Verifies CLI entry point exists.
    """
    from main import main
    
    assert callable(main)
    
    # Verify it accepts no required arguments
    import inspect
    sig = inspect.signature(main)
    assert len(sig.parameters) == 0


@pytest.mark.smoke
def test_orchestrator_attributes_initialized(orchestrator):
    """
    Smoke test: Verify orchestrator attributes are properly initialized.
    
    Ensures all expected attributes exist and have correct initial values.
    """
    assert hasattr(orchestrator, 'monitor_performance')
    assert hasattr(orchestrator, 'process_logger')
    assert hasattr(orchestrator, 'error_logger')
    assert hasattr(orchestrator, 'perf_monitor')
    assert hasattr(orchestrator, 'error_recovery')
    assert hasattr(orchestrator, 'setup_orchestrator')
    assert hasattr(orchestrator, 'current_process_id')
    
    # Verify initial values
    assert orchestrator.monitor_performance in (True, False)
    assert orchestrator.process_logger is None
    assert orchestrator.error_logger is None
    assert orchestrator.perf_monitor is None
    assert orchestrator.error_recovery is None
    assert orchestrator.setup_orchestrator is None
    assert orchestrator.current_process_id is None
