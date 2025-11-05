"""
===============================================
Comprehensive pytest suite for create_logs.py
===============================================

Sections:
---------
1. Unit tests - Individual method testing
2. Integration tests - Component interaction testing  
3. Edge case tests - Boundary conditions
4. Smoke tests - Basic functionality verification

Available markers:
------------------
unit, integration, edge_case, smoke

Test Coverage:
--------------
- LoggingInfrastructure initialization
- Table creation (create_all_tables)
- Process logging (log_process_start, log_process_end)
- Error logging (log_error)
- Data lineage tracking (log_data_lineage)
- Process status retrieval (get_process_status)
- Session management and resource cleanup
- JSONB metadata handling
- Error handling and exception propagation

How to Execute:
---------------
All tests:          pytest tests/tests_setup/test_create_logs.py -v
By category:        pytest tests/tests_setup/test_create_logs.py -m unit
Multiple markers:   pytest tests/tests_setup/test_create_logs.py -m "unit or integration"
Specific test:      pytest tests/tests_setup/test_create_logs.py::test_create_all_tables_success
With coverage:      pytest tests/tests_setup/test_create_logs.py --cov=setup.create_logs

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

# ====================
# Helper Functions
# ====================

@contextmanager
def create_mock_session(session):
    """
    Helper to create a proper context manager for mocking _get_session.
    
    This ensures the session is properly yielded and exceptions are handled.
    """
    yield session


# ====================
# Mock Helper Classes
# ====================

class FakeSession:
    """
    Mock SQLAlchemy Session for testing ORM operations.
    
    Simulates SQLAlchemy Session behavior including add, commit, rollback, 
    close, and query operations.
    """
    
    def __init__(self, query_results=None, flush_side_effect=None, commit_side_effect=None):
        self.added_objects = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.query_results = query_results or {}
        self._flush_side_effect = flush_side_effect
        self._commit_side_effect = commit_side_effect
        self._flushed_objects = []
    
    def add(self, obj):
        """Add an object to the session."""
        self.added_objects.append(obj)
    
    def flush(self):
        """
        Flush pending changes, simulating ID assignment.
        """
        if self._flush_side_effect:
            raise self._flush_side_effect
        
        # Simulate ID assignment
        for obj in self.added_objects:
            if obj not in self._flushed_objects:
                if hasattr(obj, 'log_id') and obj.log_id is None:
                    obj.log_id = 1
                elif hasattr(obj, 'error_id') and obj.error_id is None:
                    obj.error_id = 100
                elif hasattr(obj, 'lineage_id') and obj.lineage_id is None:
                    obj.lineage_id = 200
                self._flushed_objects.append(obj)
    
    def commit(self):
        """Commit the current transaction."""
        if self._commit_side_effect:
            raise self._commit_side_effect
        self.committed = True
    
    def rollback(self):
        """Rollback the current transaction."""
        self.rolled_back = True
    
    def close(self):
        """Close the session."""
        self.closed = True
    
    def query(self, model):
        """Return a mock query object."""
        return FakeQuery(self.query_results.get(model, None))


class FakeQuery:
    """Mock SQLAlchemy Query."""
    
    def __init__(self, result=None):
        self.result = result
        self._filter_kwargs = {}
    
    def filter_by(self, **kwargs):
        """Filter query by keyword arguments."""
        self._filter_kwargs = kwargs
        return self
    
    def first(self):
        """Return the first result."""
        return self.result


class FakeEngine:
    """Mock SQLAlchemy Engine."""
    
    def __init__(self):
        self.disposed = False
    
    def dispose(self):
        """Dispose of the engine."""
        self.disposed = True


class FakeMetadata:
    """Mock SQLAlchemy MetaData."""
    
    def __init__(self):
        self.create_all_called = False
        self.create_all_engine = None
        self.create_all_checkfirst = None
    
    def create_all(self, engine, checkfirst=False):
        """Simulate creating all tables."""
        self.create_all_called = True
        self.create_all_engine = engine
        self.create_all_checkfirst = checkfirst


# ===============
# 1. UNIT TESTS
# ===============

@pytest.mark.unit
def test_logging_infrastructure_init(logging_infra_factory):
    """Unit test: Verify LoggingInfrastructure initializes correctly."""
    logs = logging_infra_factory(
        host='testhost',
        port=5433,
        user='testuser',
        password='testpass',
        database='testdb'
    )
    
    assert logs.host == 'testhost'
    assert logs.port == 5433
    assert logs.user == 'testuser'
    assert logs.password == 'testpass'
    assert logs.database == 'testdb'
    assert logs._engine is None
    assert logs._session_factory is None


@pytest.mark.unit
def test_get_engine_creates_engine_once(patch_logging_create_engine, logging_infra_factory):
    """Unit test: Verify _get_engine creates engine only once (singleton pattern)."""
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    logs = logging_infra_factory()
    
    # First call should create engine
    engine1 = logs._get_engine()
    assert engine1 is fake_engine
    assert patch_logging_create_engine.call_count == 1
    
    # Second call should return same engine
    engine2 = logs._get_engine()
    assert engine2 is engine1
    assert patch_logging_create_engine.call_count == 1


@pytest.mark.unit
def test_create_all_tables_success(patch_logging_create_engine, logging_infra_factory):
    """Unit test: Verify create_all_tables creates all logging tables successfully."""
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    fake_metadata = FakeMetadata()
    
    with patch('setup.create_logs.Base') as mock_base:
        mock_base.metadata = fake_metadata
        
        logs = logging_infra_factory()
        results = logs.create_all_tables()
        
        # Verify metadata.create_all was called
        assert fake_metadata.create_all_called
        assert fake_metadata.create_all_engine is fake_engine
        assert fake_metadata.create_all_checkfirst is True
        
        # Verify results dict contains all expected tables
        expected_tables = ['process_log', 'error_log', 'data_lineage', 
                          'performance_metrics', 'configuration_log']
        for table in expected_tables:
            assert table in results
            assert results[table] is True


@pytest.mark.unit
def test_create_all_tables_raises_on_error(patch_logging_create_engine, logging_infra_factory):
    """Unit test: Verify create_all_tables raises LoggingInfrastructureError on SQL errors."""
    from setup.create_logs import LoggingInfrastructureError
    
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    fake_metadata = FakeMetadata()
    
    def raise_error(*args, **kwargs):
        raise SQLAlchemyError("Table creation failed")
    
    fake_metadata.create_all = raise_error
    
    with patch('setup.create_logs.Base') as mock_base:
        mock_base.metadata = fake_metadata
        
        logs = logging_infra_factory()
        
        with pytest.raises(LoggingInfrastructureError) as exc_info:
            logs.create_all_tables()
        
        assert "Failed to create logging tables" in str(exc_info.value)


@pytest.mark.unit
def test_log_process_start_creates_process_log(logging_infra_factory):
    """Unit test: Verify log_process_start creates ProcessLog with correct attributes."""
    from models.logs_models import ProcessLog
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        metadata = {'key1': 'value1', 'key2': 123}
        
        log_id = logs.log_process_start(
            process_name='test_process',
            process_description='Test description',
            source_system='CRM',
            target_layer='bronze',
            created_by='test_user',
            metadata=metadata
        )
        
        # Verify ProcessLog was created
        assert len(fake_session.added_objects) == 1
        process_log = fake_session.added_objects[0]
        assert isinstance(process_log, ProcessLog)
        assert process_log.process_name == 'test_process'
        assert process_log.process_description == 'Test description'
        assert process_log.source_system == 'CRM'
        assert process_log.target_layer == 'bronze'
        assert process_log.created_by == 'test_user'
        assert process_log.process_metadata == metadata  # Should be dict, not string
        assert log_id == 1


@pytest.mark.unit
def test_log_process_start_metadata_as_dict(logging_infra_factory):
    """Unit test: Verify metadata is stored as dict (not string) in JSONB column."""
    from models.logs_models import ProcessLog
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        metadata = {'nested': {'key': 'value'}, 'array': [1, 2, 3]}
        
        logs.log_process_start(process_name='test', metadata=metadata)
        
        process_log = fake_session.added_objects[0]
        
        # Critical: metadata should be dict, not string
        assert isinstance(process_log.process_metadata, dict)
        assert process_log.process_metadata == metadata
        assert process_log.process_metadata != str(metadata)


@pytest.mark.unit
def test_log_process_start_raises_on_error(logging_infra_factory):
    """Unit test: Verify log_process_start raises LoggingInfrastructureError on failure."""
    from setup.create_logs import LoggingInfrastructureError
    
    fake_session = FakeSession(flush_side_effect=SQLAlchemyError("DB error"))
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        with pytest.raises(LoggingInfrastructureError) as exc_info:
            logs.log_process_start(process_name='test')
        
        assert "Failed to log process start" in str(exc_info.value)


@pytest.mark.unit
def test_log_process_end_updates_process_log(logging_infra_factory):
    """Unit test: Verify log_process_end updates ProcessLog with completion details."""
    from models.logs_models import ProcessLog
    
    mock_process_log = ProcessLog(
        log_id=1,
        process_name='test_process',
        status='RUNNING'
    )
    
    fake_session = FakeSession(query_results={ProcessLog: mock_process_log})
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        logs.log_process_end(
            log_id=1,
            status='SUCCESS',
            rows_processed=1000,
            rows_inserted=800,
            rows_updated=150,
            rows_deleted=50
        )
        
        # Verify ProcessLog was updated
        assert mock_process_log.status == 'SUCCESS'
        assert mock_process_log.rows_processed == 1000
        assert mock_process_log.rows_inserted == 800
        assert mock_process_log.rows_updated == 150
        assert mock_process_log.rows_deleted == 50
        
        # Critical: end_time should be datetime object, not func.now()
        assert isinstance(mock_process_log.end_time, datetime)


@pytest.mark.unit
def test_log_process_end_process_not_found(logging_infra_factory):
    """Unit test: Verify log_process_end raises error when process log not found."""
    from setup.create_logs import LoggingInfrastructureError; from models.logs_models import ProcessLog
    
    fake_session = FakeSession(query_results={ProcessLog: None})
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        with pytest.raises(LoggingInfrastructureError) as exc_info:
            logs.log_process_end(log_id=999, status='SUCCESS')
        
        assert "Process log with ID 999 not found" in str(exc_info.value)


@pytest.mark.unit
def test_log_error_creates_error_log(logging_infra_factory):
    """Unit test: Verify log_error creates ErrorLog with correct attributes."""
    from models.logs_models import ErrorLog
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        error_id = logs.log_error(
            process_log_id=1,
            error_message='Test error',
            error_level='ERROR',
            error_code='E001',
            error_detail='Detailed stack trace'
        )
        
        assert len(fake_session.added_objects) == 1
        error_log = fake_session.added_objects[0]
        assert isinstance(error_log, ErrorLog)
        assert error_log.process_log_id == 1
        assert error_log.error_message == 'Test error'
        assert error_log.error_level == 'ERROR'
        assert error_log.error_code == 'E001'
        assert error_id == 100


@pytest.mark.unit
def test_log_data_lineage_creates_lineage(logging_infra_factory):
    """Unit test: Verify log_data_lineage creates DataLineage with correct attributes."""
    from models.logs_models import DataLineage
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        lineage_id = logs.log_data_lineage(
            process_log_id=1,
            source_schema='bronze',
            source_table='raw_data',
            target_schema='silver',
            target_table='clean_data',
            transformation_logic='UPPER(raw_col)',
            record_count=5000
        )
        
        assert len(fake_session.added_objects) == 1
        lineage = fake_session.added_objects[0]
        assert isinstance(lineage, DataLineage)
        assert lineage.source_schema == 'bronze'
        assert lineage.source_table == 'raw_data'
        assert lineage.target_schema == 'silver'
        assert lineage.target_table == 'clean_data'
        assert lineage.transformation_logic == 'UPPER(raw_col)'
        assert lineage.record_count == 5000
        assert lineage_id == 200


@pytest.mark.unit
def test_get_process_status_returns_status_dict(logging_infra_factory):
    """Unit test: Verify get_process_status returns correct status dictionary."""
    from models.logs_models import ProcessLog
    
    mock_process_log = ProcessLog(
        log_id=1,
        process_name='test_process',
        status='SUCCESS',
        start_time=datetime(2025, 11, 5, 10, 0, 0),
        end_time=datetime(2025, 11, 5, 10, 5, 0),
        rows_processed=1000
    )
    
    fake_session = FakeSession(query_results={ProcessLog: mock_process_log})
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        result = logs.get_process_status(log_id=1)
        
        assert result is not None
        assert result['log_id'] == 1
        assert result['process_name'] == 'test_process'
        assert result['status'] == 'SUCCESS'
        assert result['rows_processed'] == 1000


@pytest.mark.unit
def test_get_process_status_returns_none_when_not_found(logging_infra_factory):
    """Unit test: Verify get_process_status returns None when process not found."""
    from models.logs_models import ProcessLog
    
    fake_session = FakeSession(query_results={ProcessLog: None})
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        result = logs.get_process_status(log_id=999)
        assert result is None


@pytest.mark.unit
def test_close_connections_disposes_engine(patch_logging_create_engine, logging_infra_factory):
    """Unit test: Verify close_connections disposes of engine."""
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    logs = logging_infra_factory()
    logs._get_engine()
    logs.close_connections()
    
    assert fake_engine.disposed
    assert logs._engine is None


@pytest.mark.unit
def test_close_connections_when_no_engine(logging_infra_factory):
    """Unit test: Verify close_connections handles no engine gracefully."""
    logs = logging_infra_factory()
    logs.close_connections()  # Should not raise
    assert logs._engine is None


# =======================
# 2. INTEGRATION TESTS
# =======================

@pytest.mark.integration
def test_session_context_manager_commits_on_success(logging_infra_factory, patch_logging_create_engine):
    """Integration test: Verify _get_session commits on successful operations."""
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch('setup.create_logs.sessionmaker') as mock_sessionmaker:
        mock_factory = Mock(return_value=fake_session)
        mock_sessionmaker.return_value = mock_factory
        
        with logs._get_session() as session:
            session.add(Mock())
        
        assert fake_session.committed
        assert fake_session.closed


@pytest.mark.integration
def test_session_context_manager_rolls_back_on_error(logging_infra_factory, patch_logging_create_engine):
    """Integration test: Verify _get_session rolls back on exceptions."""
    fake_engine = FakeEngine()
    patch_logging_create_engine.return_value = fake_engine
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch('setup.create_logs.sessionmaker') as mock_sessionmaker:
        mock_factory = Mock(return_value=fake_session)
        mock_sessionmaker.return_value = mock_factory
        
        with pytest.raises(ValueError):
            with logs._get_session() as session:
                raise ValueError("Test error")
        
        assert fake_session.rolled_back
        assert fake_session.closed
        assert not fake_session.committed


# =====================
# 3. EDGE CASE TESTS
# =====================

@pytest.mark.edge_case
def test_log_process_start_with_none_metadata(logging_infra_factory):
    """Edge case test: Verify log_process_start handles None metadata correctly."""
    from models.logs_models import ProcessLog
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        logs.log_process_start(process_name='test', metadata=None)
        
        process_log = fake_session.added_objects[0]
        assert process_log.process_metadata is None
        assert process_log.process_metadata != "None"


@pytest.mark.edge_case
def test_log_process_start_with_empty_dict_metadata(logging_infra_factory):
    """Edge case test: Verify log_process_start handles empty dict metadata."""
    from models.logs_models import ProcessLog
    
    fake_session = FakeSession()
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        logs.log_process_start(process_name='test', metadata={})
        
        process_log = fake_session.added_objects[0]
        assert process_log.process_metadata == {}
        assert process_log.process_metadata != "{}"


@pytest.mark.edge_case
def test_log_process_end_with_failed_status_and_error_message(logging_infra_factory):
    """Edge case test: Verify log_process_end stores error message on failure."""
    from models.logs_models import ProcessLog
    
    mock_process_log = ProcessLog(log_id=1, process_name='test', status='RUNNING')
    fake_session = FakeSession(query_results={ProcessLog: mock_process_log})
    logs = logging_infra_factory()
    
    with patch.object(logs, '_get_session', return_value=create_mock_session(fake_session)):
        logs.log_process_end(
            log_id=1,
            status='FAILED',
            error_message='Database connection timeout'
        )
        
        assert mock_process_log.status == 'FAILED'
        assert mock_process_log.error_message == 'Database connection timeout'


# =================
# 4. SMOKE TESTS
# =================

@pytest.mark.smoke
def test_logging_infrastructure_can_be_imported():
    """Smoke test: Verify LoggingInfrastructure can be imported."""
    from setup.create_logs import LoggingInfrastructure
    assert LoggingInfrastructure is not None


@pytest.mark.smoke
def test_logging_infrastructure_can_be_instantiated():
    """Smoke test: Verify LoggingInfrastructure can be instantiated."""
    from setup.create_logs import LoggingInfrastructure
    logs = LoggingInfrastructure()
    assert logs is not None
    assert logs.host == 'localhost'


@pytest.mark.smoke
def test_all_orm_models_can_be_imported():
    """Smoke test: Verify all ORM models can be imported."""
    from setup.create_logs import (
        ConfigurationLog,
        DataLineage,
        ErrorLog,
        PerformanceMetrics,
        ProcessLog,
    )
    assert all([ProcessLog, ErrorLog, DataLineage, PerformanceMetrics, ConfigurationLog])


@pytest.mark.smoke
def test_logging_infrastructure_error_can_be_raised():
    """Smoke test: Verify LoggingInfrastructureError can be raised."""
    from setup.create_logs import LoggingInfrastructureError
    with pytest.raises(LoggingInfrastructureError):
        raise LoggingInfrastructureError("Test error")


@pytest.mark.smoke
def test_base_metadata_exists():
    """Smoke test: Verify SQLAlchemy Base has metadata."""
    from models.logs_models import Base
    assert hasattr(Base, 'metadata')
    assert Base.metadata is not None
