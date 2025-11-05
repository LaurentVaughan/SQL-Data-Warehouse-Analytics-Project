"""
=================================================
Comprehensive pytest suite for audit_logger.py
=================================================

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
ProcessLogger:
- Process lifecycle (start_process, end_process, update_process_metrics)
- Active process queries (get_active_processes)
- Process history queries (get_process_history)
- Session management and resource cleanup
- JSONB metadata handling
- Error handling and exception propagation
- UTC timezone handling

ConfigurationLogger:
- Configuration change logging (log_config_change)
- Configuration history queries (get_config_history)
- Session management and resource cleanup

BatchLogger:
- Batch lifecycle (start_batch, log_batch_progress, end_batch)
- Integration with ProcessLogger

How to Execute:
---------------
All tests:          pytest tests/tests_logs/test_audit_logger.py -v
By category:        pytest tests/tests_logs/test_audit_logger.py -m unit
Multiple markers:   pytest tests/tests_logs/test_audit_logger.py -m "unit or integration"
Specific test:      pytest tests/tests_logs/test_audit_logger.py::test_start_process_success
With coverage:      pytest tests/tests_logs/test_audit_logger.py --cov=logs.audit_logger

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

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
                elif hasattr(obj, 'config_log_id') and obj.config_log_id is None:
                    obj.config_log_id = 200
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
        self._filters = []
        self._order_by_called = False
    
    def filter_by(self, **kwargs):
        """Filter query by keyword arguments."""
        self._filter_kwargs = kwargs
        return self
    
    def filter(self, *args):
        """Filter query by conditions."""
        self._filters.extend(args)
        return self
    
    def order_by(self, *args):
        """Order query results."""
        self._order_by_called = True
        return self
    
    def first(self):
        """Return the first result."""
        if isinstance(self.result, list):
            return self.result[0] if self.result else None
        return self.result
    
    def all(self):
        """Return all results."""
        if isinstance(self.result, list):
            return self.result
        return [self.result] if self.result else []


class FakeEngine:
    """Mock SQLAlchemy Engine."""
    
    def __init__(self):
        self.disposed = False
    
    def dispose(self):
        """Dispose of the engine."""
        self.disposed = True


class FakeProcessLog:
    """Mock ProcessLog ORM object."""
    
    def __init__(self, log_id=None, **kwargs):
        self.log_id = log_id
        self.process_name = kwargs.get('process_name', 'test_process')
        self.process_description = kwargs.get('process_description', None)
        self.start_time = kwargs.get('start_time', datetime.now(timezone.utc))
        self.end_time = kwargs.get('end_time', None)
        self.status = kwargs.get('status', 'RUNNING')
        self.rows_processed = kwargs.get('rows_processed', None)
        self.rows_inserted = kwargs.get('rows_inserted', None)
        self.rows_updated = kwargs.get('rows_updated', None)
        self.rows_deleted = kwargs.get('rows_deleted', None)
        self.source_system = kwargs.get('source_system', None)
        self.target_layer = kwargs.get('target_layer', None)
        self.error_message = kwargs.get('error_message', None)
        self.process_metadata = kwargs.get('process_metadata', None)
        self.created_by = kwargs.get('created_by', 'system')


class FakeConfigurationLog:
    """Mock ConfigurationLog ORM object."""
    
    def __init__(self, config_log_id=None, **kwargs):
        self.config_log_id = config_log_id
        self.config_key = kwargs.get('config_key', 'test_key')
        self.old_value = kwargs.get('old_value', 'old')
        self.new_value = kwargs.get('new_value', 'new')
        self.change_reason = kwargs.get('change_reason', 'test')
        self.changed_by = kwargs.get('changed_by', 'system')
        self.change_timestamp = kwargs.get('change_timestamp', datetime.now(timezone.utc))
        self.environment = kwargs.get('environment', 'production')


# ===============
# 1. UNIT TESTS
# ===============

@pytest.mark.unit
def test_get_session_context_manager(process_logger_factory):
    """
    Unit test: Verify _get_session provides proper context manager.
    
    Tests that ProcessLogger._get_session() returns a context manager
    that properly commits on success and closes the session.
    """
    from unittest.mock import MagicMock, patch
    
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    
    logger = process_logger_factory()
    
    # Mock _get_engine to avoid database connection
    with patch.object(logger, "_get_engine", return_value=mock_engine):
        with patch("sqlalchemy.orm.sessionmaker", return_value=mock_sessionmaker):
            with logger._get_session() as session:
                assert session is mock_session
            
            assert mock_session.committed is True
            assert mock_session.closed is True


@pytest.mark.unit
def test_get_session_rollback_on_exception(process_logger_factory):
    """
    Unit test: Verify _get_session rolls back on exception.
    
    Tests that ProcessLogger._get_session() properly handles exceptions
    by rolling back the transaction before re-raising.
    """
    from unittest.mock import MagicMock, patch
    
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    
    logger = process_logger_factory()
    
    # Mock _get_engine to avoid database connection
    with patch.object(logger, "_get_engine", return_value=mock_engine):
        with patch("sqlalchemy.orm.sessionmaker", return_value=mock_sessionmaker):
            with pytest.raises(RuntimeError):
                with logger._get_session() as session:
                    raise RuntimeError("Test error")
            
            assert mock_session.rolled_back is True
            assert mock_session.closed is True


@pytest.mark.unit
def test_start_process_success(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify start_process creates ProcessLog entry and returns ID.
    
    Tests that ProcessLogger.start_process() creates a ProcessLog ORM object,
    adds it to the session, flushes to get the ID, and returns the log_id.
    """
    from logs.audit_logger import ProcessLogger
    
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        log_id = logger.start_process(
            process_name='test_process',
            process_description='Test description',
            source_system='CRM',
            target_layer='bronze',
            created_by='test_user',
            metadata={'key': 'value'}
        )
    
    assert log_id == 1
    assert len(mock_session.added_objects) == 1
    assert mock_session.committed is True
    
    # Verify ProcessLog attributes
    process_log = mock_session.added_objects[0]
    assert process_log.process_name == 'test_process'
    assert process_log.process_description == 'Test description'
    assert process_log.source_system == 'CRM'
    assert process_log.target_layer == 'bronze'
    assert process_log.created_by == 'test_user'
    assert process_log.process_metadata == {'key': 'value'}  # Dict, not JSON string


@pytest.mark.unit
def test_start_process_metadata_as_dict(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify start_process passes metadata dict directly to JSONB.
    
    Tests that ProcessLogger.start_process() passes the metadata dictionary
    directly to the JSONB column without json.dumps() conversion.
    """
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    metadata = {'batch_id': 123, 'source': 'file.csv', 'nested': {'key': 'value'}}
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        log_id = logger.start_process(
            process_name='test_process',
            metadata=metadata
        )
    
    # Verify metadata is stored as dict, not string
    process_log = mock_session.added_objects[0]
    assert isinstance(process_log.process_metadata, dict)
    assert process_log.process_metadata == metadata
    assert process_log.process_metadata['nested']['key'] == 'value'


@pytest.mark.unit
def test_start_process_raises_on_sqlalchemy_error(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify start_process raises AuditLoggerError on database errors.
    
    Tests that ProcessLogger.start_process() catches SQLAlchemyError and
    re-raises as AuditLoggerError with appropriate context.
    """
    from logs.audit_logger import AuditLoggerError
    
    mock_session = FakeSession(flush_side_effect=SQLAlchemyError("DB error"))
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        with pytest.raises(AuditLoggerError) as exc_info:
            logger.start_process(process_name='test_process')
        
        assert "Failed to log process start" in str(exc_info.value)


@pytest.mark.unit
def test_end_process_success(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify end_process updates ProcessLog with completion data.
    
    Tests that ProcessLogger.end_process() retrieves the ProcessLog by ID
    and updates it with end_time, status, and metrics using UTC timezone.
    """
    from setup.create_logs import ProcessLog
    
    fake_process_log = FakeProcessLog(log_id=1)
    mock_session = FakeSession(query_results={ProcessLog: fake_process_log})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        logger.end_process(
            log_id=1,
            status='SUCCESS',
            rows_processed=1000,
            rows_inserted=800,
            rows_updated=150,
            rows_deleted=50,
            error_message=None
        )
    
    # Verify updates
    assert fake_process_log.status == 'SUCCESS'
    assert fake_process_log.rows_processed == 1000
    assert fake_process_log.rows_inserted == 800
    assert fake_process_log.rows_updated == 150
    assert fake_process_log.rows_deleted == 50
    assert fake_process_log.error_message is None
    assert fake_process_log.end_time is not None
    # Verify UTC timezone usage
    assert fake_process_log.end_time.tzinfo == timezone.utc
    assert mock_session.committed is True


@pytest.mark.unit
def test_end_process_not_found_raises_error(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify end_process raises error when log_id not found.
    
    Tests that ProcessLogger.end_process() raises AuditLoggerError when
    the specified log_id doesn't exist in the database.
    """
    from logs.audit_logger import AuditLoggerError
    from setup.create_logs import ProcessLog
    
    mock_session = FakeSession(query_results={ProcessLog: None})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        with pytest.raises(AuditLoggerError) as exc_info:
            logger.end_process(log_id=999, status='SUCCESS')
        
        assert "Process log with ID 999 not found" in str(exc_info.value)


@pytest.mark.unit
def test_update_process_metrics_success(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify update_process_metrics updates ProcessLog metrics.
    
    Tests that ProcessLogger.update_process_metrics() retrieves the ProcessLog
    by ID and updates the specified metrics fields.
    """
    from setup.create_logs import ProcessLog
    
    fake_process_log = FakeProcessLog(log_id=1)
    mock_session = FakeSession(query_results={ProcessLog: fake_process_log})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        logger.update_process_metrics(
            log_id=1,
            rows_processed=500,
            rows_inserted=400,
            rows_updated=75,
            rows_deleted=25
        )
    
    # Verify updates
    assert fake_process_log.rows_processed == 500
    assert fake_process_log.rows_inserted == 400
    assert fake_process_log.rows_updated == 75
    assert fake_process_log.rows_deleted == 25
    assert mock_session.committed is True


@pytest.mark.unit
def test_update_process_metrics_partial_update(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify update_process_metrics handles partial updates.
    
    Tests that ProcessLogger.update_process_metrics() only updates the
    specified metrics, leaving None values unchanged.
    """
    from setup.create_logs import ProcessLog
    
    fake_process_log = FakeProcessLog(log_id=1, rows_processed=100)
    mock_session = FakeSession(query_results={ProcessLog: fake_process_log})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        logger.update_process_metrics(
            log_id=1,
            rows_processed=200  # Only update this field
        )
    
    # Verify only specified field was updated
    assert fake_process_log.rows_processed == 200
    # Other fields remain None
    assert fake_process_log.rows_inserted is None
    assert fake_process_log.rows_updated is None
    assert fake_process_log.rows_deleted is None


@pytest.mark.unit
def test_get_active_processes_returns_running_processes(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify get_active_processes returns only RUNNING processes.
    
    Tests that ProcessLogger.get_active_processes() queries for processes
    with status='RUNNING' and returns them as dictionaries.
    """
    from setup.create_logs import ProcessLog
    
    active_processes = [
        FakeProcessLog(log_id=1, process_name='process_1', status='RUNNING'),
        FakeProcessLog(log_id=2, process_name='process_2', status='RUNNING')
    ]
    
    mock_session = FakeSession(query_results={ProcessLog: active_processes})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        results = logger.get_active_processes()
    
    assert len(results) == 2
    assert results[0]['log_id'] == 1
    assert results[0]['process_name'] == 'process_1'
    assert results[1]['log_id'] == 2
    assert results[1]['process_name'] == 'process_2'


@pytest.mark.unit
def test_get_process_history_with_filters(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify get_process_history applies filters correctly.
    
    Tests that ProcessLogger.get_process_history() applies filters for
    process_name, status, and time range using UTC timezone.
    """
    from setup.create_logs import ProcessLog
    
    now = datetime.now(timezone.utc)
    processes = [
        FakeProcessLog(
            log_id=1,
            process_name='test_process',
            status='SUCCESS',
            start_time=now - timedelta(days=1),
            end_time=now
        )
    ]
    
    mock_session = FakeSession(query_results={ProcessLog: processes})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        results = logger.get_process_history(
            process_name='test_process',
            days=7,
            status='SUCCESS'
        )
    
    assert len(results) == 1
    assert results[0]['log_id'] == 1
    assert results[0]['process_name'] == 'test_process'
    assert results[0]['status'] == 'SUCCESS'
    assert 'duration_seconds' in results[0]


@pytest.mark.unit
def test_close_connections_disposes_engine(patch_audit_create_engine, process_logger_factory):
    """
    Unit test: Verify close_connections disposes engine and clears reference.
    
    Tests that ProcessLogger.close_connections() properly cleans up database
    resources by calling engine.dispose() and resetting _engine to None.
    """
    mock_engine = FakeEngine()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    # Force engine creation
    _ = logger._get_engine()
    assert logger._engine is not None
    
    logger.close_connections()
    assert logger._engine is None
    assert mock_engine.disposed is True


@pytest.mark.unit
def test_configuration_logger_initialization(config_logger_factory):
    """
    Unit test: Verify ConfigurationLogger initializes with correct parameters.
    
    Tests that ConfigurationLogger can be instantiated with default connection
    parameters and that internal state is properly initialized.
    """
    logger = config_logger_factory()
    
    assert logger.host == "localhost"
    assert logger.port == 5432
    assert logger.user == "postgres"
    assert logger.password == "secret"
    assert logger.database == "warehouse"
    assert logger._engine is None
    assert logger._session_factory is None


@pytest.mark.unit
def test_log_config_change_success(patch_audit_create_engine, config_logger_factory):
    """
    Unit test: Verify log_config_change creates ConfigurationLog entry.
    
    Tests that ConfigurationLogger.log_config_change() creates a
    ConfigurationLog ORM object with the correct attributes.
    """
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = config_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        config_log_id = logger.log_config_change(
            config_key='batch_size',
            old_value='1000',
            new_value='5000',
            change_reason='Performance optimization',
            changed_by='admin',
            environment='production'
        )
    
    assert config_log_id == 200
    assert len(mock_session.added_objects) == 1
    assert mock_session.committed is True
    
    # Verify ConfigurationLog attributes
    config_log = mock_session.added_objects[0]
    assert config_log.config_key == 'batch_size'
    assert config_log.old_value == '1000'
    assert config_log.new_value == '5000'
    assert config_log.change_reason == 'Performance optimization'
    assert config_log.changed_by == 'admin'
    assert config_log.environment == 'production'


@pytest.mark.unit
def test_get_config_history_with_filters(patch_audit_create_engine, config_logger_factory):
    """
    Unit test: Verify get_config_history applies filters correctly.
    
    Tests that ConfigurationLogger.get_config_history() applies filters for
    config_key, environment, and time range using UTC timezone.
    """
    from setup.create_logs import ConfigurationLog
    
    now = datetime.now(timezone.utc)
    config_changes = [
        FakeConfigurationLog(
            config_log_id=1,
            config_key='batch_size',
            old_value='1000',
            new_value='5000',
            change_timestamp=now - timedelta(days=1),
            environment='production'
        )
    ]
    
    mock_session = FakeSession(query_results={ConfigurationLog: config_changes})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = config_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        results = logger.get_config_history(
            config_key='batch_size',
            days=30,
            environment='production'
        )
    
    assert len(results) == 1
    assert results[0]['config_log_id'] == 1
    assert results[0]['config_key'] == 'batch_size'
    assert results[0]['old_value'] == '1000'
    assert results[0]['new_value'] == '5000'


@pytest.mark.unit
def test_batch_logger_initialization(batch_logger_factory):
    """
    Unit test: Verify BatchLogger initializes with ProcessLogger instance.
    
    Tests that BatchLogger can be instantiated with a ProcessLogger and
    that internal state is properly initialized.
    """
    mock_process_logger = MagicMock()
    batch_logger = batch_logger_factory(process_logger=mock_process_logger)
    
    assert batch_logger.process_logger is mock_process_logger
    assert batch_logger.current_batch_id is None
    assert batch_logger.current_process_id is None


@pytest.mark.unit
def test_start_batch_creates_batch_id_with_utc(batch_logger_factory):
    """
    Unit test: Verify start_batch creates batch_id using UTC timestamp.
    
    Tests that BatchLogger.start_batch() generates a batch_id with UTC
    timestamp and starts a process with batch metadata.
    """
    mock_process_logger = MagicMock()
    mock_process_logger.start_process.return_value = 123
    
    batch_logger = batch_logger_factory(process_logger=mock_process_logger)
    
    batch_id = batch_logger.start_batch(
        batch_name='test_batch',
        total_records=10000,
        batch_size=1000,
        source_system='CRM',
        target_layer='bronze'
    )
    
    # Verify batch_id format
    assert batch_id.startswith('batch_')
    assert batch_logger.current_batch_id == batch_id
    assert batch_logger.current_process_id == 123
    
    # Verify start_process was called with metadata
    mock_process_logger.start_process.assert_called_once()
    call_kwargs = mock_process_logger.start_process.call_args[1]
    assert call_kwargs['process_name'] == 'test_batch'
    assert call_kwargs['source_system'] == 'CRM'
    assert call_kwargs['target_layer'] == 'bronze'
    assert isinstance(call_kwargs['metadata'], dict)
    assert call_kwargs['metadata']['total_records'] == 10000
    assert call_kwargs['metadata']['batch_size'] == 1000
    assert call_kwargs['metadata']['estimated_batches'] == 10


@pytest.mark.unit
def test_log_batch_progress_updates_metrics(batch_logger_factory):
    """
    Unit test: Verify log_batch_progress updates process metrics.
    
    Tests that BatchLogger.log_batch_progress() calls update_process_metrics
    with the current process_id and cumulative metrics.
    """
    mock_process_logger = MagicMock()
    batch_logger = batch_logger_factory(process_logger=mock_process_logger)
    batch_logger.current_process_id = 123
    
    batch_logger.log_batch_progress(
        batch_number=5,
        records_processed=5000,
        records_inserted=4500,
        records_updated=400,
        records_deleted=100
    )
    
    # Verify update_process_metrics was called
    mock_process_logger.update_process_metrics.assert_called_once_with(
        log_id=123,
        rows_processed=5000,
        rows_inserted=4500,
        rows_updated=400,
        rows_deleted=100
    )


@pytest.mark.unit
def test_end_batch_completes_process(batch_logger_factory):
    """
    Unit test: Verify end_batch completes the process and resets state.
    
    Tests that BatchLogger.end_batch() calls end_process with the final
    status and count, then resets internal state.
    """
    mock_process_logger = MagicMock()
    batch_logger = batch_logger_factory(process_logger=mock_process_logger)
    batch_logger.current_batch_id = 'batch_123'
    batch_logger.current_process_id = 456
    
    batch_logger.end_batch(
        status='SUCCESS',
        final_record_count=10000,
        error_message=None
    )
    
    # Verify end_process was called
    mock_process_logger.end_process.assert_called_once_with(
        log_id=456,
        status='SUCCESS',
        rows_processed=10000,
        error_message=None
    )
    
    # Verify state was reset
    assert batch_logger.current_batch_id is None
    assert batch_logger.current_process_id is None


# ======================
# 2. INTEGRATION TESTS
# ======================

@pytest.mark.integration
def test_process_lifecycle_integration(patch_audit_create_engine, process_logger_factory):
    """
    Integration test: Verify complete process lifecycle from start to end.
    
    Tests the full workflow of starting a process, updating metrics,
    and ending the process.
    """
    from setup.create_logs import ProcessLog
    
    fake_process_log = FakeProcessLog(log_id=1)
    mock_session = FakeSession(query_results={ProcessLog: fake_process_log})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        # Start process
        log_id = logger.start_process(
            process_name='integration_test',
            process_description='Full lifecycle test',
            source_system='CRM',
            target_layer='bronze',
            metadata={'test': 'data'}
        )
        
        assert log_id == 1
        
        # Update metrics
        logger.update_process_metrics(
            log_id=log_id,
            rows_processed=500
        )
        
        assert fake_process_log.rows_processed == 500
        
        # End process
        logger.end_process(
            log_id=log_id,
            status='SUCCESS',
            rows_processed=1000
        )
        
        assert fake_process_log.status == 'SUCCESS'
        assert fake_process_log.rows_processed == 1000
        assert fake_process_log.end_time is not None


@pytest.mark.integration
def test_batch_logger_integration_with_process_logger(patch_audit_create_engine, process_logger_factory):
    """
    Integration test: Verify BatchLogger integrates with ProcessLogger.
    
    Tests the complete workflow of batch processing using both
    BatchLogger and ProcessLogger.
    """
    from logs.audit_logger import BatchLogger
    from setup.create_logs import ProcessLog
    
    fake_process_log = FakeProcessLog(log_id=1)
    mock_session = FakeSession(query_results={ProcessLog: fake_process_log})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    process_logger = process_logger_factory()
    batch_logger = BatchLogger(process_logger)
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        # Start batch
        batch_id = batch_logger.start_batch(
            batch_name='integration_batch',
            total_records=10000,
            batch_size=1000,
            source_system='ERP',
            target_layer='silver'
        )
        
        assert batch_id is not None
        assert batch_logger.current_process_id == 1
        
        # Log progress
        batch_logger.log_batch_progress(
            batch_number=5,
            records_processed=5000,
            records_inserted=4500
        )
        
        assert fake_process_log.rows_processed == 5000
        assert fake_process_log.rows_inserted == 4500
        
        # End batch
        batch_logger.end_batch(
            status='SUCCESS',
            final_record_count=10000
        )
        
        assert fake_process_log.status == 'SUCCESS'
        assert batch_logger.current_batch_id is None


@pytest.mark.integration
def test_configuration_change_tracking_integration(patch_audit_create_engine, config_logger_factory):
    """
    Integration test: Verify configuration change tracking workflow.
    
    Tests the complete workflow of logging configuration changes and
    retrieving change history.
    """
    from setup.create_logs import ConfigurationLog
    
    config_change = FakeConfigurationLog(config_log_id=1, config_key='batch_size')
    mock_session = FakeSession(query_results={ConfigurationLog: [config_change]})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = config_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        # Log configuration change
        config_log_id = logger.log_config_change(
            config_key='batch_size',
            old_value='1000',
            new_value='5000',
            change_reason='Performance tuning',
            changed_by='admin'
        )
        
        assert config_log_id == 200
        
        # Get configuration history
        history = logger.get_config_history(config_key='batch_size')
        
        assert len(history) == 1
        assert history[0]['config_key'] == 'batch_size'


# ====================
# 3. EDGE CASE TESTS
# ====================

@pytest.mark.edge_case
def test_start_process_with_none_metadata(patch_audit_create_engine, process_logger_factory):
    """
    Edge case test: Verify start_process handles None metadata.
    
    Tests that ProcessLogger.start_process() correctly handles None
    metadata without errors.
    """
    mock_session = FakeSession()
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        log_id = logger.start_process(
            process_name='test_process',
            metadata=None
        )
    
    assert log_id == 1
    process_log = mock_session.added_objects[0]
    assert process_log.process_metadata is None


@pytest.mark.edge_case
def test_get_active_processes_empty_result(patch_audit_create_engine, process_logger_factory):
    """
    Edge case test: Verify get_active_processes handles empty results.
    
    Tests that ProcessLogger.get_active_processes() returns an empty list
    when no active processes exist.
    """
    from setup.create_logs import ProcessLog
    
    mock_session = FakeSession(query_results={ProcessLog: []})
    mock_sessionmaker = MagicMock(return_value=mock_session)
    mock_engine = MagicMock()
    patch_audit_create_engine.return_value = mock_engine
    
    logger = process_logger_factory()
    
    with patch("logs.audit_logger.sessionmaker", return_value=mock_sessionmaker):
        results = logger.get_active_processes()
    
    assert results == []


@pytest.mark.edge_case
def test_end_batch_without_current_process_id(batch_logger_factory):
    """
    Edge case test: Verify end_batch handles missing process_id gracefully.
    
    Tests that BatchLogger.end_batch() doesn't raise errors when
    current_process_id is None.
    """
    mock_process_logger = MagicMock()
    batch_logger = batch_logger_factory(process_logger=mock_process_logger)
    batch_logger.current_process_id = None
    
    # Should not raise
    batch_logger.end_batch(
        status='SUCCESS',
        final_record_count=0
    )
    
    # Verify end_process was not called
    mock_process_logger.end_process.assert_not_called()


# ================
# 4. SMOKE TESTS
# ================

@pytest.mark.smoke
def test_smoke_import_process_logger():
    """
    Smoke test: Verify ProcessLogger can be imported and instantiated.
    
    Tests basic import and instantiation of ProcessLogger without
    database connections.
    """
    from logs.audit_logger import ProcessLogger
    
    logger = ProcessLogger(
        host='localhost',
        user='postgres',
        password='test',
        database='test_db'
    )
    
    assert logger is not None
    assert logger.host == 'localhost'


@pytest.mark.smoke
def test_smoke_import_configuration_logger():
    """
    Smoke test: Verify ConfigurationLogger can be imported and instantiated.
    
    Tests basic import and instantiation of ConfigurationLogger without
    database connections.
    """
    from logs.audit_logger import ConfigurationLogger
    
    logger = ConfigurationLogger(
        host='localhost',
        user='postgres',
        password='test',
        database='test_db'
    )
    
    assert logger is not None
    assert logger.host == 'localhost'


@pytest.mark.smoke
def test_smoke_import_batch_logger():
    """
    Smoke test: Verify BatchLogger can be imported and instantiated.
    
    Tests basic import and instantiation of BatchLogger with a mock
    ProcessLogger.
    """
    from logs.audit_logger import BatchLogger
    
    mock_process_logger = MagicMock()
    batch_logger = BatchLogger(mock_process_logger)
    
    assert batch_logger is not None
    assert batch_logger.process_logger is mock_process_logger


@pytest.mark.smoke
def test_smoke_close_connections_without_engine(process_logger_factory):
    """
    Smoke test: Verify close_connections handles no engine gracefully.
    
    Tests that ProcessLogger.close_connections() doesn't raise errors
    when called without an engine being created.
    """
    logger = process_logger_factory()
    
    # Should not raise
    logger.close_connections()
    assert logger._engine is None


@pytest.mark.smoke
def test_smoke_audit_logger_error_exception():
    """
    Smoke test: Verify AuditLoggerError can be raised and caught.
    
    Tests that the custom AuditLoggerError exception works correctly.
    """
    from logs.audit_logger import AuditLoggerError
    
    with pytest.raises(AuditLoggerError) as exc_info:
        raise AuditLoggerError("Test error message")
    
    assert "Test error message" in str(exc_info.value)
