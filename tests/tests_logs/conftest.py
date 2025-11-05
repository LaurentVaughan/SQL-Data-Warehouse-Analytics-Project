"""
Shared fixtures and mocking helpers for logs/ module tests.

Key fixtures:
- process_logger_factory: factory for creating ProcessLogger instances
- config_logger_factory: factory for creating ConfigurationLogger instances
- batch_logger_factory: factory for creating BatchLogger instances

Note: This conftest avoids importing the logs package (__init__.py) to prevent
circular import issues with setup_orchestrator. All imports go directly to the module.
"""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def process_logger_factory():
    """
    Factory that creates a ProcessLogger instance with default params.
    Tests will patch create_engine separately.
    
    Note: Import is done inside the fixture to avoid circular imports.
    """
    def factory(**overrides):
        # Import here to avoid circular import issues
        from logs.audit_logger import ProcessLogger
        
        params = dict(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            database="warehouse"
        )
        params.update(overrides)
        return ProcessLogger(**params)

    return factory


@pytest.fixture
def config_logger_factory():
    """
    Factory that creates a ConfigurationLogger instance with default params.
    Tests will patch create_engine separately.
    
    Note: Import is done inside the fixture to avoid circular imports.
    """
    def factory(**overrides):
        # Import here to avoid circular import issues
        from logs.audit_logger import ConfigurationLogger
        
        params = dict(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            database="warehouse"
        )
        params.update(overrides)
        return ConfigurationLogger(**params)

    return factory


@pytest.fixture
def batch_logger_factory():
    """
    Factory that creates a BatchLogger instance with a mock ProcessLogger.
    
    Note: Import is done inside the fixture to avoid circular imports.
    """
    def factory(process_logger=None):
        # Import here to avoid circular import issues
        from unittest.mock import MagicMock

        from logs.audit_logger import BatchLogger
        
        if process_logger is None:
            process_logger = MagicMock()
        return BatchLogger(process_logger)

    return factory


@pytest.fixture
def patch_audit_create_engine(monkeypatch):
    """
    Patch create_engine in logs.audit_logger module.
    
    Returns a MagicMock that can be configured by tests to return
    a mock engine.
    
    Usage in tests:
        patch_audit_create_engine.return_value = mock_engine
    """
    from unittest.mock import MagicMock
    
    mock_create_engine = MagicMock()
    monkeypatch.setattr("logs.audit_logger.create_engine", mock_create_engine)
    return mock_create_engine


@pytest.fixture
def patch_error_create_engine(monkeypatch):
    """
    Patch create_engine in logs.error_handler module.
    
    Returns a MagicMock that can be configured by tests to return
    a mock engine.
    
    Usage in tests:
        patch_error_create_engine.return_value = mock_engine
    """
    mock_create_engine = MagicMock()
    monkeypatch.setattr("logs.error_handler.create_engine", mock_create_engine)
    return mock_create_engine


@pytest.fixture
def patch_analyzer_create_engine(monkeypatch):
    """
    Patch create_engine for ErrorAnalyzer in logs.error_handler module.
    
    This is the same as patch_error_create_engine but with a different name
    for clarity in ErrorAnalyzer tests.
    
    Returns a MagicMock that can be configured by tests to return
    a mock engine.
    """
    mock_create_engine = MagicMock()
    monkeypatch.setattr("logs.error_handler.create_engine", mock_create_engine)
    return mock_create_engine


@pytest.fixture
def mock_session():
    """Mock SQLAlchemy Session for testing database operations."""
    from unittest.mock import Mock
    from sqlalchemy.orm import Session
    
    session = Mock(spec=Session)
    # Mock flush to set error_id/metric_id/lineage_id
    def mock_flush():
        # Set ID on the object that was added
        if session.add.called:
            added_obj = session.add.call_args[0][0]
            if hasattr(added_obj, 'error_id'):
                added_obj.error_id = 123
            elif hasattr(added_obj, 'metric_id'):
                added_obj.metric_id = 456
            elif hasattr(added_obj, 'lineage_id'):
                added_obj.lineage_id = 789
    
    session.flush.side_effect = mock_flush
    return session


@pytest.fixture
def mock_error_logger(patch_error_create_engine):
    """Create ErrorLogger with mocked engine."""
    from logs.error_handler import ErrorLogger
    
    mock_engine = MagicMock()
    patch_error_create_engine.return_value = mock_engine
    
    logger = ErrorLogger(
        host="localhost",
        port=5432,
        database="warehouse",
        user="postgres",
        password="secret",
    )
    return logger


@pytest.fixture
def mock_error_analyzer(patch_analyzer_create_engine):
    """Create ErrorAnalyzer with mocked engine."""
    from logs.error_handler import ErrorAnalyzer
    
    mock_engine = MagicMock()
    patch_analyzer_create_engine.return_value = mock_engine
    
    analyzer = ErrorAnalyzer(
        host="localhost",
        port=5432,
        database="warehouse",
        user="postgres",
        password="secret",
    )
    return analyzer


@pytest.fixture
def patch_performance_create_engine(monkeypatch):
    """Patch create_engine in logs.performance_monitor module."""
    mock_create_engine = MagicMock()
    monkeypatch.setattr("logs.performance_monitor.create_engine", mock_create_engine)
    return mock_create_engine


@pytest.fixture
def mock_performance_monitor(patch_performance_create_engine):
    """Create PerformanceMonitor with mocked engine."""
    from logs.performance_monitor import PerformanceMonitor
    
    mock_engine = MagicMock()
    patch_performance_create_engine.return_value = mock_engine
    
    monitor = PerformanceMonitor(
        host="localhost",
        port=5432,
        database="warehouse",
        user="postgres",
        password="secret",
    )
    return monitor


@pytest.fixture
def patch_lineage_create_engine(monkeypatch):
    """Patch create_engine in logs.data_lineage module."""
    mock_create_engine = MagicMock()
    monkeypatch.setattr("logs.data_lineage.create_engine", mock_create_engine)
    return mock_create_engine


@pytest.fixture
def mock_lineage_tracker(patch_lineage_create_engine):
    """Create LineageTracker with mocked engine."""
    from logs.data_lineage import LineageTracker
    
    mock_engine = MagicMock()
    patch_lineage_create_engine.return_value = mock_engine
    
    tracker = LineageTracker(
        host="localhost",
        port=5432,
        database="warehouse",
        user="postgres",
        password="secret",
    )
    return tracker
