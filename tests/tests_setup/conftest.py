"""
Shared fixtures and mocking helpers for tests.

Key fixtures:
- patch_create_engine: patches sqlalchemy.create_engine to return a fake engine.
- dummy_sql_module: patches sql.ddl and sql.query_builder functions used by create_database.
- db_creator_factory: returns a DatabaseCreator instance wired to the patched engine.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=False)
def patch_create_engine():
    """
    Patch sqlalchemy.create_engine and yield a factory to build fake engines / connections.
    Use in tests that need to simulate engine/connection behavior.
    """
    with patch("setup.create_database.create_engine") as mock_create_engine:
        yield mock_create_engine


@pytest.fixture
def dummy_sql_module(monkeypatch):
    """
    Patch sql.ddl and sql.query_builder functions used by create_database.
    Returns a dict of the strings these functions will return so tests can assert executed SQL.
    """
    # Basic SQL templates that the module will return
    payload = {
        "create_sql": "CREATE DATABASE dummydb TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'en_GB.UTF-8' LC_CTYPE 'en_GB.UTF-8';",
        "drop_sql": "DROP DATABASE IF EXISTS dummydb;",
        "terminate_sql": "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'dummydb';",
        "exists_sql": "SELECT 1 FROM pg_database WHERE datname='dummydb';",
        "count_conn_sql": "SELECT count(*) FROM pg_stat_activity WHERE datname='dummydb';"
    }

    # patch functions
    monkeypatch.setattr("setup.create_database.create_database_sql", lambda **kwargs: payload["create_sql"])
    monkeypatch.setattr("setup.create_database.drop_database_sql", lambda **kwargs: payload["drop_sql"])
    monkeypatch.setattr("setup.create_database.terminate_connections_sql", lambda db: payload["terminate_sql"])
    monkeypatch.setattr("setup.create_database.check_database_exists_sql", lambda db: payload["exists_sql"])
    monkeypatch.setattr("setup.create_database.count_database_connections_sql", lambda db: payload["count_conn_sql"])

    return payload


@pytest.fixture
def db_creator_factory():
    """
    Factory that creates a DatabaseCreator with default params. Tests will patch create_engine separately.
    """
    from setup.create_database import DatabaseCreator

    def factory(**overrides):
        params = dict(
            host="localhost",
            port=5432,
            user="postgres",
            password="secret",
            admin_db="postgres",
            target_db="dummydb"
        )
        params.update(overrides)
        return DatabaseCreator(**params)

    return factory
