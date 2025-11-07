"""
======================================================
Comprehensive pytest suite for setup_orchestrator.py
======================================================

Sections:
---------
1. Unit tests - Individual method testing
2. Integration tests - Component interaction testing
3. System tests - Full orchestration workflow
4. Edge case tests - Boundary conditions
5. Smoke tests - Basic functionality verification

Available markers:
------------------
unit, integration, system, edge_case, smoke

Test Coverage:
--------------
SetupOrchestrator:
- Initialization and configuration validation
- Component initialization (DatabaseCreator, SchemaCreator, LoggingInfrastructure)
- Setup step tracking and logging
- Database creation orchestration
- Schema creation orchestration
- Logging infrastructure setup
- Sample table creation
- Complete setup workflow (run_complete_setup)
- Rollback functionality
- Error handling and exception propagation

How to Execute:
---------------
All tests:          pytest tests/tests_setup/test_setup_orchestrator.py -v
By category:        pytest tests/tests_setup/test_setup_orchestrator.py -m unit
Multiple markers:   pytest tests/tests_setup/test_setup_orchestrator.py -m "unit or integration"
Specific test:      pytest tests/tests_setup/test_setup_orchestrator.py::test_init_success
With coverage:      pytest tests/tests_setup/test_setup_orchestrator.py --cov=setup.setup_orchestrator

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from setup.setup_orchestrator import SetupError, SetupOrchestrator

# ====================
# Mock Helper Classes
# ====================

class FakeConfig:
    """Mock config object for testing."""
    def __init__(self, missing_attr=None):
        """
        Args:
            missing_attr: Attribute name to simulate as missing (None value)
        """
        self.db_host = 'localhost' if missing_attr != 'db_host' else None
        self.db_port = 5432 if missing_attr != 'db_port' else None
        self.db_user = 'postgres' if missing_attr != 'db_user' else None
        self.db_password = 'secret' if missing_attr != 'db_password' else None
        self.db_name = 'postgres' if missing_attr != 'db_name' else None
        self.warehouse_db_name = 'warehouse_db' if missing_attr != 'warehouse_db_name' else None


class FakeDatabaseCreator:
    """Mock DatabaseCreator for testing.
    
    Test Scenarios Supported:
    1. exists_result=True: Database already exists (skip creation)
    2. exists_result=False + create_succeeds=True: Creation succeeds, verification succeeds
    3. exists_result=False + create_succeeds=False: Creation succeeds but verification fails
    4. exists_result=False + should_raise_on_create=True: Creation raises exception
    """
    def __init__(self, *args, **kwargs):
        self.create_called = False
        self.drop_called = False
        
        # Control flags for testing different scenarios
        self.exists_result = False  # Database existence before operation
        self.create_succeeds = True  # Whether creation should succeed
        self.should_raise_on_create = False  # Whether to raise exception
        self.exception_to_raise = Exception("Database creation failed")  # Exception to raise
        self.drop_result = True
        
    def check_database_exists(self) -> bool:
        """Check if database exists - matches real implementation."""
        return self.exists_result
    
    def create_database(self) -> None:
        """Create database - matches real implementation (returns None, raises on error).
        
        Behavior based on test flags:
        - If should_raise_on_create=True: Raise exception
        - If create_succeeds=True: Set exists_result=True (successful creation)
        - If create_succeeds=False: Leave exists_result unchanged (verification will fail)
        """
        self.create_called = True
        
        # Scenario: Creation should fail with exception
        if self.should_raise_on_create:
            raise self.exception_to_raise
        
        # Scenario: Creation succeeds (database will exist after)
        if self.create_succeeds:
            self.exists_result = True
        
        # Scenario: Creation completes but verification will fail
        # (exists_result stays False - simulates race condition or permission issue)
    
    def drop_database(self, force: bool = True) -> bool:
        """Drop database - matches real implementation."""
        self.drop_called = True
        return self.drop_result
    
    def terminate_connections(self) -> int:
        """Terminate database connections - matches real implementation."""
        return 0
    
    def close_connections(self) -> None:
        """Close connections - matches real implementation."""
        pass


class FakeSchemaCreator:
    """Mock SchemaCreator for testing."""
    def __init__(self, *args, **kwargs):
        self.create_all_called = False
        self.drop_all_called = False
        self.create_all_result = {
            'bronze': True,
            'silver': True,
            'gold': True,
            'logs': True
        }
        self.drop_all_result = True
        
    def create_all_schemas(self):
        self.create_all_called = True
        return self.create_all_result
    
    def drop_all_schemas(self):
        self.drop_all_called = True
        return self.drop_all_result


class FakeLoggingInfrastructure:
    """Mock LoggingInfrastructure for testing."""
    def __init__(self, *args, **kwargs):
        self.create_all_called = False
        self.create_all_result = {
            'process_log': True,
            'configuration_log': True,
            'data_lineage': True,
            'error_log': True,
            'performance_metrics': True
        }
        
    def create_all_tables(self):
        self.create_all_called = True
        return self.create_all_result


class FakeProcessLogger:
    """Mock ProcessLogger for testing."""
    def __init__(self, *args, **kwargs):
        self.process_counter = 100
        self.started_processes = []
        self.ended_processes = []
        
    def start_process(self, process_name, process_description, **kwargs):
        self.process_counter += 1
        self.started_processes.append({
            'id': self.process_counter,
            'name': process_name,
            'description': process_description,
            'metadata': kwargs
        })
        return self.process_counter
    
    def end_process(self, log_id, status, **kwargs):
        self.ended_processes.append({
            'log_id': log_id,
            'status': status,
            'metadata': kwargs
        })


# ====================
# Fixtures
# ====================

@pytest.fixture
def mock_config():
    """Provide mock configuration."""
    with patch('setup.setup_orchestrator.config', FakeConfig()):
        yield FakeConfig()


@pytest.fixture
def orchestrator(mock_config):
    """Provide SetupOrchestrator instance with mocked config."""
    return SetupOrchestrator()


@pytest.fixture
def mock_components():
    """Provide mocked component classes."""
    with patch('setup.setup_orchestrator.DatabaseCreator', FakeDatabaseCreator), \
         patch('setup.setup_orchestrator.SchemaCreator', FakeSchemaCreator), \
         patch('setup.setup_orchestrator.LoggingInfrastructure', FakeLoggingInfrastructure), \
         patch('setup.setup_orchestrator.ProcessLogger', FakeProcessLogger):
        yield


# ===============
# 1. UNIT TESTS
# ===============

@pytest.mark.unit
def test_init_success(mock_config):
    """
    Test SetupOrchestrator initialization with valid configuration.
    
    Verifies orchestrator initializes successfully with complete config.
    """
    orchestrator = SetupOrchestrator()
    
    assert orchestrator.host == 'localhost'
    assert orchestrator.port == 5432
    assert orchestrator.user == 'postgres'
    assert orchestrator.password == 'secret'
    assert orchestrator.admin_db == 'postgres'
    assert orchestrator.target_db == 'warehouse_db'
    assert orchestrator.db_creator is None  # Not initialized yet
    assert orchestrator.schema_creator is None
    assert orchestrator.logging_infrastructure is None
    assert orchestrator.process_logger is None
    assert orchestrator.setup_steps == []
    assert orchestrator.current_step is None


@pytest.mark.unit
def test_init_missing_config():
    """
    Test SetupOrchestrator initialization with missing configuration.
    
    Verifies SetupError is raised when required config is missing.
    """
    with patch('setup.setup_orchestrator.config', FakeConfig(missing_attr='db_host')):
        with pytest.raises(SetupError) as exc_info:
            SetupOrchestrator()
        
        assert 'Missing required configuration' in str(exc_info.value)
        assert 'Database host' in str(exc_info.value)


@pytest.mark.unit
def test_validate_config_all_present(mock_config):
    """
    Test configuration validation with all required settings present.
    
    Verifies validation passes when all config is complete.
    """
    orchestrator = SetupOrchestrator()
    # If we get here without exception, validation passed
    assert orchestrator.host == 'localhost'


@pytest.mark.unit
def test_validate_config_multiple_missing():
    """
    Test configuration validation with multiple missing settings.
    
    Verifies all missing settings are reported in error message.
    """
    config = FakeConfig()
    config.db_host = None
    config.db_password = None
    
    with patch('setup.setup_orchestrator.config', config):
        with pytest.raises(SetupError) as exc_info:
            SetupOrchestrator()
        
        error_msg = str(exc_info.value)
        assert 'Database host' in error_msg
        assert 'Database password' in error_msg


@pytest.mark.unit
def test_initialize_components(orchestrator, mock_components):
    """
    Test component initialization.
    
    Verifies all components are properly initialized with correct parameters.
    """
    orchestrator._initialize_components()
    
    assert isinstance(orchestrator.db_creator, FakeDatabaseCreator)
    assert isinstance(orchestrator.schema_creator, FakeSchemaCreator)
    assert isinstance(orchestrator.logging_infrastructure, FakeLoggingInfrastructure)


@pytest.mark.unit
def test_start_setup_step_no_logger(orchestrator):
    """
    Test starting setup step without process logger.
    
    Verifies step tracking works even without logger initialized.
    """
    process_id = orchestrator._start_setup_step(
        "test_step",
        "Test step description"
    )
    
    # Should return 0 when no logger available
    assert process_id == 0
    assert orchestrator.current_step == "test_step"


@pytest.mark.unit
def test_start_setup_step_with_logger(orchestrator, mock_components):
    """
    Test starting setup step with process logger.
    
    Verifies step is logged when logger is available.
    """
    orchestrator._initialize_components()
    orchestrator.process_logger = FakeProcessLogger()
    
    process_id = orchestrator._start_setup_step(
        "test_step",
        "Test step description"
    )
    
    assert process_id > 0
    assert orchestrator.current_step == "test_step"
    assert len(orchestrator.process_logger.started_processes) == 1
    started = orchestrator.process_logger.started_processes[0]
    assert started['name'] == "setup_test_step"
    assert started['description'] == "Test step description"


@pytest.mark.unit
def test_end_setup_step_success(orchestrator, mock_components):
    """
    Test ending setup step with success status.
    
    Verifies successful step completion is tracked.
    """
    orchestrator._initialize_components()
    orchestrator.process_logger = FakeProcessLogger()
    
    # Start a step first to populate setup_steps
    process_id = orchestrator._start_setup_step("test_step", "Test description")
    
    orchestrator._end_setup_step(process_id, 'SUCCESS')
    
    assert orchestrator.current_step == "test_step"
    # Check that setup_steps was updated
    step = orchestrator.setup_steps[0]
    assert step['step_name'] == 'test_step'
    assert step['status'] == 'SUCCESS'
    assert len(orchestrator.process_logger.ended_processes) == 1
    ended = orchestrator.process_logger.ended_processes[0]
    assert ended['log_id'] == process_id
    assert ended['status'] == 'SUCCESS'


@pytest.mark.unit
def test_end_setup_step_failure(orchestrator, mock_components):
    """
    Test ending setup step with failure status.
    
    Verifies failed step completion is tracked with error message.
    """
    orchestrator._initialize_components()
    orchestrator.process_logger = FakeProcessLogger()
    orchestrator.current_step = "test_step"
    
    orchestrator._end_setup_step(102, 'FAILED', error_message='Test error')
    
    assert orchestrator.current_step == "test_step"
    assert "test_step" not in orchestrator.setup_steps  # Failed steps not added
    assert len(orchestrator.process_logger.ended_processes) == 1
    ended = orchestrator.process_logger.ended_processes[0]
    assert ended['status'] == 'FAILED'
    assert ended['metadata']['error_message'] == 'Test error'


@pytest.mark.unit
def test_create_database_success_new_database(orchestrator, mock_components):
    """
    Scenario 1: Database doesn't exist, creation succeeds, verification succeeds.
    
    Expected: create_database() called, check_database_exists() returns True after creation.
    Result: Method returns True.
    """
    orchestrator._initialize_components()
    
    # Setup: Database doesn't exist initially, creation will succeed
    orchestrator.db_creator.exists_result = False
    orchestrator.db_creator.create_succeeds = True
    
    result = orchestrator.create_database()
    
    assert result is True
    assert orchestrator.db_creator.create_called is True
    assert orchestrator.db_creator.exists_result is True  # Verify flag set after creation


@pytest.mark.unit
def test_create_database_already_exists(orchestrator, mock_components):
    """
    Scenario 2: Database already exists.
    
    Expected: check_database_exists() returns True immediately, skip creation.
    Result: Method returns True, create_database() never called.
    """
    orchestrator._initialize_components()
    
    # Setup: Database already exists
    orchestrator.db_creator.exists_result = True
    
    result = orchestrator.create_database()
    
    assert result is True
    assert orchestrator.db_creator.create_called is False  # Creation skipped


@pytest.mark.unit
def test_create_database_verification_fails(orchestrator, mock_components):
    """
    Scenario 3: Database doesn't exist, creation succeeds, but verification fails.
    
    Expected: create_database() called, but check_database_exists() still returns False.
    Result: Method returns False (verification failed).
    """
    orchestrator._initialize_components()
    
    # Setup: Database doesn't exist, creation completes but verification fails
    orchestrator.db_creator.exists_result = False
    orchestrator.db_creator.create_succeeds = False  # Verification will fail
    
    result = orchestrator.create_database()
    
    assert result is False
    assert orchestrator.db_creator.create_called is True
    assert orchestrator.db_creator.exists_result is False  # Still False after creation


@pytest.mark.unit
def test_create_database_exception_raised(orchestrator, mock_components):
    """
    Scenario 4: Database doesn't exist, creation raises exception.
    
    Expected: create_database() raises exception (e.g., DatabaseCreationError).
    Result: Method raises SetupError with appropriate message.
    """
    orchestrator._initialize_components()
    
    # Setup: Database doesn't exist, creation will raise exception
    orchestrator.db_creator.exists_result = False
    orchestrator.db_creator.should_raise_on_create = True
    orchestrator.db_creator.exception_to_raise = Exception("Permission denied")
    
    with pytest.raises(SetupError) as exc_info:
        orchestrator.create_database()
    
    assert "Database creation failed" in str(exc_info.value)
    assert orchestrator.db_creator.create_called is True


@pytest.mark.unit
def test_create_schemas_success(orchestrator, mock_components):
    """
    Test schema creation success.
    
    Verifies all schemas are created successfully.
    """
    orchestrator._initialize_components()
    orchestrator.schema_creator.create_all_result = {
        'bronze': True,
        'silver': True,
        'gold': True,
        'logs': True
    }
    
    result = orchestrator.create_schemas()
    
    assert result is True
    assert orchestrator.schema_creator.create_all_called is True


@pytest.mark.unit
def test_create_schemas_failure(orchestrator, mock_components):
    """
    Test schema creation failure.
    
    Verifies method returns False when schema creation fails.
    """
    orchestrator._initialize_components()
    orchestrator.schema_creator.create_all_result = {
        'bronze': True,
        'silver': False,  # One schema fails
        'gold': True,
        'logs': True
    }
    
    result = orchestrator.create_schemas()
    
    assert result is False


@pytest.mark.unit
def test_create_logging_infrastructure_success(orchestrator, mock_components):
    """
    Test logging infrastructure creation success.
    
    Verifies logging tables are created successfully.
    """
    orchestrator._initialize_components()
    # Leave the default dict result from FakeLoggingInfrastructure
    
    result = orchestrator.create_logging_infrastructure()
    
    assert result is True
    assert orchestrator.logging_infrastructure.create_all_called is True


@pytest.mark.unit
def test_create_logging_infrastructure_failure(orchestrator, mock_components):
    """
    Test logging infrastructure creation failure.
    
    Verifies method returns False when table creation fails.
    """
    orchestrator._initialize_components()
    orchestrator.logging_infrastructure.create_all_result = {
        'process_log': True,
        'configuration_log': False,  # One table fails
        'data_lineage': True,
        'error_log': True,
        'performance_metrics': True
    }
    
    result = orchestrator.create_logging_infrastructure()
    
    assert result is False


# =======================
# 2. INTEGRATION TESTS
# =======================

@pytest.mark.integration
def test_run_complete_setup_success_no_samples(orchestrator, mock_components):
    """
    Test complete setup workflow without samples.
    
    Verifies full setup orchestration works end-to-end.
    """
    results = orchestrator.run_complete_setup(include_samples=False)
    
    assert isinstance(results, dict)
    assert 'database' in results
    assert 'schemas' in results
    assert 'logging' in results
    assert 'samples' not in results  # Not included
    
    # All steps should succeed
    assert results['database'] is True
    assert results['schemas'] is True
    assert results['logging'] is True


@pytest.mark.integration
def test_run_complete_setup_success_with_samples(orchestrator, mock_components):
    """
    Test complete setup workflow with sample tables.
    
    Verifies samples are created when requested.
    """
    # Mock sample creation
    with patch.object(orchestrator, 'create_sample_medallion_tables', return_value=True):
        results = orchestrator.run_complete_setup(include_samples=True)
        
        assert 'samples' in results
        assert results['samples'] is True


@pytest.mark.integration
def test_run_complete_setup_database_failure_verification(orchestrator, mock_components):
    """
    Test setup workflow when database verification fails (returns False).
    
    Scenario: Database creation completes but verification fails.
    Expected: results['database'] = False, setup stops early.
    """
    orchestrator._initialize_components()
    
    # Database doesn't exist, creation will complete but verification fails
    orchestrator.db_creator.exists_result = False
    orchestrator.db_creator.create_succeeds = False  # Verification will fail
    
    results = orchestrator.run_complete_setup()
    
    assert results['database'] is False
    assert 'schemas' not in results  # Should stop after database failure


@pytest.mark.integration
def test_run_complete_setup_database_failure_exception(orchestrator, mock_components):
    """
    Test setup workflow when database creation raises exception.
    
    Scenario: Database creation raises exception (e.g., permission denied).
    Expected: Exception caught, results['database'] = False, setup stops early.
    """
    orchestrator._initialize_components()
    
    # Database doesn't exist, creation will raise exception
    orchestrator.db_creator.exists_result = False
    orchestrator.db_creator.should_raise_on_create = True
    orchestrator.db_creator.exception_to_raise = Exception("Permission denied")
    
    results = orchestrator.run_complete_setup()
    
    assert results['database'] is False
    assert 'schemas' not in results  # Should stop after database failure


@pytest.mark.integration
def test_run_complete_setup_components_initialized(orchestrator, mock_components):
    """
    Test that run_complete_setup initializes components.
    
    Verifies components are available after setup.
    """
    orchestrator.run_complete_setup(include_samples=False)
    
    assert orchestrator.db_creator is not None
    assert orchestrator.schema_creator is not None
    assert orchestrator.logging_infrastructure is not None


@pytest.mark.integration
def test_setup_step_tracking(orchestrator, mock_components):
    """
    Test that setup steps are tracked throughout workflow.
    
    Verifies setup_steps list accumulates completed steps.
    """
    orchestrator._initialize_components()
    orchestrator.process_logger = FakeProcessLogger()
    
    # Complete a few steps
    orchestrator.create_database()
    orchestrator.create_schemas()
    
    assert len(orchestrator.setup_steps) >= 2
    step_names = [step['step_name'] for step in orchestrator.setup_steps]
    assert 'create_database' in step_names
    assert 'create_schemas' in step_names


# ====================
# 3. SYSTEM TESTS
# ====================

@pytest.mark.system
def test_complete_setup_workflow_integration(mock_components):
    """
    System test: Complete setup from initialization to completion.
    
    Verifies entire workflow including all components.
    """
    with patch('setup.setup_orchestrator.config', FakeConfig()):
        orchestrator = SetupOrchestrator()
        results = orchestrator.run_complete_setup(include_samples=False)
        
        # Verify all expected steps completed
        assert all(results.values())
        assert orchestrator.db_creator is not None
        assert orchestrator.schema_creator is not None
        assert orchestrator.logging_infrastructure is not None


@pytest.mark.system
def test_rollback_functionality(orchestrator, mock_components):
    """
    System test: Rollback after setup.
    
    Verifies rollback drops schemas and optionally database.
    """
    orchestrator._initialize_components()
    
    # First do setup
    orchestrator.run_complete_setup(include_samples=False)
    
    # Then rollback
    result = orchestrator.rollback_setup(keep_database=True)
    
    assert orchestrator.schema_creator.drop_all_called is True
    assert orchestrator.db_creator.drop_called is False  # Database kept


@pytest.mark.system
def test_rollback_drop_database(orchestrator, mock_components):
    """
    System test: Rollback including database drop.
    
    Verifies full rollback drops database too.
    """
    orchestrator._initialize_components()
    
    result = orchestrator.rollback_setup(keep_database=False)
    
    assert orchestrator.db_creator.drop_called is True


# ====================
# 4. EDGE CASE TESTS
# ====================

@pytest.mark.edge_case
def test_multiple_setup_runs(orchestrator, mock_components):
    """
    Test running setup multiple times.
    
    Verifies orchestrator handles repeated setup calls.
    """
    # First setup
    results1 = orchestrator.run_complete_setup(include_samples=False)
    assert all(results1.values())
    
    # Second setup (database already exists)
    orchestrator.db_creator.exists_result = True
    results2 = orchestrator.run_complete_setup(include_samples=False)
    assert all(results2.values())


@pytest.mark.edge_case
def test_partial_setup_recovery(orchestrator, mock_components):
    """
    Test recovery from partial setup state.
    
    Verifies orchestrator can continue after partial completion.
    """
    orchestrator._initialize_components()
    
    # Database exists but schemas don't
    orchestrator.db_creator.exists_result = True
    
    result = orchestrator.create_database()
    assert result is True  # Should handle existing database


@pytest.mark.edge_case
def test_empty_setup_steps_list(orchestrator):
    """
    Test orchestrator with no completed steps.
    
    Verifies setup_steps list starts empty.
    """
    assert orchestrator.setup_steps == []
    assert orchestrator.current_step is None


@pytest.mark.edge_case
def test_process_logger_initialization_timing(orchestrator, mock_components):
    """
    Test process logger initialization at different points.
    
    Verifies logger can be initialized during setup workflow.
    """
    orchestrator._initialize_components()
    
    # Logger not set initially
    assert orchestrator.process_logger is None
    
    # Mock ProcessLogger to fail during creation so _start_setup_step returns 0
    with patch('setup.setup_orchestrator.ProcessLogger', side_effect=Exception("Logger not ready")):
        process_id = orchestrator._start_setup_step("test", "description")
        assert process_id == 0
    
    # Now set a working logger
    orchestrator.process_logger = FakeProcessLogger()
    
    # Should now use the logger and return actual process ID
    process_id2 = orchestrator._start_setup_step("test2", "description2")
    assert process_id2 == 101  # First ID from FakeProcessLogger
    assert len(orchestrator.process_logger.started_processes) == 1


# =================
# 5. SMOKE TESTS
# =================

@pytest.mark.smoke
def test_module_imports():
    """
    Smoke test: Verify module imports work.
    
    Ensures module can be imported and classes are available.
    """
    from setup.setup_orchestrator import SetupError, SetupOrchestrator
    
    assert SetupOrchestrator is not None
    assert issubclass(SetupError, Exception)


@pytest.mark.smoke
def test_basic_initialization(mock_config):
    """
    Smoke test: Basic orchestrator initialization.
    
    Quick verification that orchestrator can be created.
    """
    orchestrator = SetupOrchestrator()
    
    assert orchestrator is not None
    assert hasattr(orchestrator, 'run_complete_setup')
    assert hasattr(orchestrator, 'create_database')
    assert hasattr(orchestrator, 'create_schemas')


@pytest.mark.smoke
def test_run_complete_setup_returns_dict(orchestrator, mock_components):
    """
    Smoke test: run_complete_setup returns results dictionary.
    
    Verifies return type and basic structure.
    """
    results = orchestrator.run_complete_setup(include_samples=False)
    
    assert isinstance(results, dict)
    assert len(results) > 0


@pytest.mark.smoke
def test_setup_error_exception():
    """
    Smoke test: SetupError can be raised and caught.
    
    Verifies custom exception works as expected.
    """
    with pytest.raises(SetupError) as exc_info:
        raise SetupError("Test error")
    
    assert "Test error" in str(exc_info.value)
