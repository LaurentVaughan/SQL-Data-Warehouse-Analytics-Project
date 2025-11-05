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
