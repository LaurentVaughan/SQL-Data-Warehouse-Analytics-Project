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
