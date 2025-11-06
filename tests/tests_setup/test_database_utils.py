"""
========================================================
Comprehensive pytest suite for utils/database_utils.py
========================================================

Sections:
---------
1. Unit tests - Individual function testing
2. Integration tests - Component interaction testing
3. Edge case tests - Boundary conditions
4. Smoke tests - Basic functionality verification

Available markers:
------------------
unit, integration, edge_case, smoke

Test Coverage:
--------------
- get_connection_string: Connection string building with various parameter combinations
- create_sqlalchemy_engine: Engine creation with URL.create and fallback
- check_database_available: Database availability verification
- wait_for_database: Retry logic and timeout handling
- verify_database_exists: Database existence checking
- get_database_connection_info: Configuration retrieval
- test_connection: Connection testing with status messages

How to Execute:
---------------
All tests:          pytest tests/tests_setup/test_database_utils.py -v
By category:        pytest tests/tests_setup/test_database_utils.py -m unit
Multiple markers:   pytest tests/tests_setup/test_database_utils.py -m "unit or integration"
Specific test:      pytest tests/tests_setup/test_database_utils.py::test_get_connection_string_defaults
With coverage:      pytest tests/tests_setup/test_database_utils.py --cov=utils.database_utils

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from psycopg2 import OperationalError
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError

from utils.database_utils import (
    DatabaseConnectionError,
    check_database_available,
    create_sqlalchemy_engine,
    get_connection_string,
    get_database_connection_info,
    verify_connection,
    verify_database_exists,
    wait_for_database,
)

# ====================
# Mock Helper Classes
# ====================

class FakeConfig:
    """Mock config object for testing."""
    def __init__(self):
        self.db_host = 'localhost'
        self.db_port = 5432
        self.db_user = 'postgres'
        self.db_password = 'secret123'
        self.db_name = 'postgres'
        self.warehouse_db_name = 'sql_retail_analytics_warehouse'


class FakeConnection:
    """Mock psycopg2 connection."""
    def __init__(self):
        self.closed = False
    
    def close(self):
        self.closed = True


class FakeSQLAlchemyConnection:
    """Mock SQLAlchemy connection for testing."""
    def __init__(self, execute_result=None):
        self.execute_result = execute_result
        self.executed_statements = []
    
    def execute(self, statement, params=None):
        """Execute a SQL statement and return mock result."""
        self.executed_statements.append((statement, params))
        return self.execute_result
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class FakeResult:
    """Mock SQLAlchemy result object."""
    def __init__(self, rows=None):
        self.rows = rows or []
    
    def fetchone(self):
        if not self.rows:
            return None
        return self.rows[0]


class FakeEngine:
    """Mock SQLAlchemy engine."""
    def __init__(self, connection=None):
        self.connection_obj = connection
        self.disposed = False
    
    def connect(self):
        return self.connection_obj
    
    def dispose(self):
        self.disposed = True


# ====================
# Fixtures
# ====================

@pytest.fixture
def mock_config():
    """Provide mock configuration."""
    with patch('utils.database_utils.config', FakeConfig()):
        yield FakeConfig()


# ===============
# 1. UNIT TESTS
# ===============

@pytest.mark.unit
def test_get_connection_string_defaults(mock_config):
    """
    Test connection string generation with default parameters.
    
    Verifies that get_connection_string uses config defaults when no
    parameters are provided.
    """
    result = get_connection_string()
    
    assert result == "postgresql://postgres:secret123@localhost:5432/postgres"


@pytest.mark.unit
def test_get_connection_string_use_warehouse(mock_config):
    """
    Test connection string generation with warehouse database.
    
    Verifies that use_warehouse=True uses warehouse_db_name from config.
    """
    result = get_connection_string(use_warehouse=True)
    
    assert result == "postgresql://postgres:secret123@localhost:5432/sql_retail_analytics_warehouse"


@pytest.mark.unit
def test_get_connection_string_custom_params(mock_config):
    """
    Test connection string generation with custom parameters.
    
    Verifies that custom parameters override config defaults.
    """
    result = get_connection_string(
        host='db.example.com',
        port=5433,
        user='admin',
        password='custom@pass',
        database='custom_db'
    )
    
    # @ symbol should be URL encoded in password
    assert result == "postgresql://admin:custom%40pass@db.example.com:5433/custom_db"


@pytest.mark.unit
def test_get_connection_string_special_chars_password(mock_config):
    """
    Test connection string handles special characters in password.
    
    Verifies that special characters in password are properly URL encoded.
    """
    result = get_connection_string(password='p@ss:w/rd!')
    
    # Special characters should be encoded
    assert 'p%40ss%3Aw%2Frd%21' in result


@pytest.mark.unit
def test_create_sqlalchemy_engine_modern_url_create(mock_config):
    """
    Test SQLAlchemy engine creation using URL.create().
    
    Verifies modern URL.create() method is used when available.
    """
    with patch('utils.database_utils.URL.create') as mock_url_create, \
         patch('utils.database_utils.create_engine') as mock_create_engine:
        
        mock_url_create.return_value = 'postgresql://...'
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        result = create_sqlalchemy_engine()
        
        # Verify URL.create was called with correct parameters
        mock_url_create.assert_called_once_with(
            drivername='postgresql',
            username='postgres',
            password='secret123',
            host='localhost',
            port=5432,
            database='postgres'
        )
        
        # Verify engine was created with proper settings
        mock_create_engine.assert_called_once()
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['pool_pre_ping'] is True
        assert call_kwargs['pool_size'] == 5
        assert call_kwargs['max_overflow'] == 10


@pytest.mark.unit
def test_create_sqlalchemy_engine_warehouse(mock_config):
    """
    Test SQLAlchemy engine creation with warehouse database.
    
    Verifies use_warehouse=True selects warehouse database.
    """
    with patch('utils.database_utils.URL.create') as mock_url_create, \
         patch('utils.database_utils.create_engine') as mock_create_engine:
        
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        create_sqlalchemy_engine(use_warehouse=True)
        
        # Verify warehouse database was used
        call_kwargs = mock_url_create.call_args[1]
        assert call_kwargs['database'] == 'sql_retail_analytics_warehouse'


@pytest.mark.unit
def test_create_sqlalchemy_engine_fallback(mock_config):
    """
    Test SQLAlchemy engine creation fallback for older versions.
    
    Verifies fallback to connection string when URL.create raises AttributeError.
    """
    with patch('utils.database_utils.URL.create', side_effect=AttributeError), \
         patch('utils.database_utils.get_connection_string') as mock_get_conn_str, \
         patch('utils.database_utils.create_engine') as mock_create_engine:
        
        mock_get_conn_str.return_value = 'postgresql://localhost/test'
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        result = create_sqlalchemy_engine(database='test_db')
        
        # Verify fallback was used
        mock_get_conn_str.assert_called_once()
        mock_create_engine.assert_called_once()


@pytest.mark.unit
def test_create_sqlalchemy_engine_custom_pool_settings(mock_config):
    """
    Test SQLAlchemy engine creation with custom pool settings.
    
    Verifies custom pool_size and max_overflow are applied.
    """
    with patch('utils.database_utils.URL.create'), \
         patch('utils.database_utils.create_engine') as mock_create_engine:
        
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        create_sqlalchemy_engine(pool_size=10, max_overflow=20, echo=True)
        
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['pool_size'] == 10
        assert call_kwargs['max_overflow'] == 20
        assert call_kwargs['echo'] is True


@pytest.mark.unit
def test_check_database_available_success(mock_config):
    """
    Test database availability check when database is available.
    
    Verifies check_database_available returns True when connection succeeds.
    """
    with patch('utils.database_utils.psycopg2.connect') as mock_connect:
        mock_conn = FakeConnection()
        mock_connect.return_value = mock_conn
        
        result = check_database_available()
        
        assert result is True
        assert mock_conn.closed is True
        mock_connect.assert_called_once_with(
            host='localhost',
            port=5432,
            user='postgres',
            password='secret123',
            database='postgres',
            connect_timeout=5
        )


@pytest.mark.unit
def test_check_database_available_failure(mock_config):
    """
    Test database availability check when database is unavailable.
    
    Verifies check_database_available returns False when connection fails.
    """
    with patch('utils.database_utils.psycopg2.connect', side_effect=OperationalError):
        result = check_database_available()
        
        assert result is False


@pytest.mark.unit
def test_check_database_available_custom_params(mock_config):
    """
    Test database availability check with custom parameters.
    
    Verifies custom connection parameters are used.
    """
    with patch('utils.database_utils.psycopg2.connect') as mock_connect:
        mock_connect.return_value = FakeConnection()
        
        check_database_available(
            host='custom.host',
            port=5433,
            user='admin',
            password='pass',
            database='test_db',
            timeout=10
        )
        
        mock_connect.assert_called_once_with(
            host='custom.host',
            port=5433,
            user='admin',
            password='pass',
            database='test_db',
            connect_timeout=10
        )


@pytest.mark.unit
def test_wait_for_database_immediate_success(mock_config):
    """
    Test wait_for_database when database is immediately available.
    
    Verifies function returns True on first attempt without retries.
    """
    with patch('utils.database_utils.check_database_available', return_value=True):
        result = wait_for_database()
        
        assert result is True


@pytest.mark.unit
def test_wait_for_database_success_after_retries(mock_config):
    """
    Test wait_for_database succeeds after multiple retries.
    
    Verifies retry logic works and function succeeds eventually.
    """
    # Fail twice, then succeed
    with patch('utils.database_utils.check_database_available', side_effect=[False, False, True]), \
         patch('utils.database_utils.time.sleep') as mock_sleep:
        
        result = wait_for_database(max_retries=5, retry_delay=1)
        
        assert result is True
        # Should have slept twice (2 failures before success)
        assert mock_sleep.call_count == 2


@pytest.mark.unit
def test_wait_for_database_max_retries_exhausted(mock_config):
    """
    Test wait_for_database raises exception when max retries exhausted.
    
    Verifies DatabaseConnectionError is raised after all retries fail.
    """
    with patch('utils.database_utils.check_database_available', return_value=False), \
         patch('utils.database_utils.time.sleep'):
        
        with pytest.raises(DatabaseConnectionError) as exc_info:
            wait_for_database(max_retries=3, retry_delay=1)
        
        assert 'did not become available' in str(exc_info.value)


@pytest.mark.unit
def test_wait_for_database_custom_params(mock_config):
    """
    Test wait_for_database with custom connection parameters.
    
    Verifies custom parameters are passed to availability check.
    """
    with patch('utils.database_utils.check_database_available', return_value=True) as mock_check:
        wait_for_database(
            host='custom.host',
            port=5433,
            user='admin',
            password='pass',
            database='test_db'
        )
        
        # Verify custom parameters were passed to check function
        mock_check.assert_called_with(
            'custom.host', 5433, 'admin', 'pass', 'test_db', 5
        )


@pytest.mark.unit
def test_verify_database_exists_true(mock_config):
    """
    Test verify_database_exists when database exists.
    
    Verifies function returns True when database is found.
    """
    fake_result = FakeResult(rows=[(1,)])
    fake_conn = FakeSQLAlchemyConnection(execute_result=fake_result)
    fake_engine = FakeEngine(connection=fake_conn)
    
    with patch('utils.database_utils.create_sqlalchemy_engine', return_value=fake_engine):
        result = verify_database_exists('test_database')
        
        assert result is True
        assert fake_engine.disposed is True
        
        # Verify query was executed
        assert len(fake_conn.executed_statements) == 1
        stmt, params = fake_conn.executed_statements[0]
        assert 'pg_database' in str(stmt)
        assert params == {'db_name': 'test_database'}


@pytest.mark.unit
def test_verify_database_exists_false(mock_config):
    """
    Test verify_database_exists when database does not exist.
    
    Verifies function returns False when database is not found.
    """
    fake_result = FakeResult(rows=[])  # No rows means database doesn't exist
    fake_conn = FakeSQLAlchemyConnection(execute_result=fake_result)
    fake_engine = FakeEngine(connection=fake_conn)
    
    with patch('utils.database_utils.create_sqlalchemy_engine', return_value=fake_engine):
        result = verify_database_exists('nonexistent_db')
        
        assert result is False


@pytest.mark.unit
def test_verify_database_exists_exception_handling(mock_config):
    """
    Test verify_database_exists handles exceptions gracefully.
    
    Verifies function returns False when exception occurs.
    """
    with patch('utils.database_utils.create_sqlalchemy_engine', side_effect=Exception("Connection failed")):
        result = verify_database_exists('test_db')
        
        assert result is False


@pytest.mark.unit
def test_verify_database_exists_uses_postgres_db(mock_config):
    """
    Test verify_database_exists connects to 'postgres' database.
    
    Verifies function uses default postgres database to check existence.
    """
    fake_result = FakeResult(rows=[])
    fake_conn = FakeSQLAlchemyConnection(execute_result=fake_result)
    fake_engine = FakeEngine(connection=fake_conn)
    
    with patch('utils.database_utils.create_sqlalchemy_engine', return_value=fake_engine) as mock_create:
        verify_database_exists('test_db', host='testhost', port=5433)
        
        # Verify engine was created with postgres database
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs['database'] == 'postgres'
        assert call_kwargs['use_warehouse'] is False
        assert call_kwargs['host'] == 'testhost'
        assert call_kwargs['port'] == 5433


@pytest.mark.unit
def test_get_database_connection_info(mock_config):
    """
    Test get_database_connection_info retrieves config values.
    
    Verifies function returns dictionary with current configuration.
    """
    result = get_database_connection_info()
    
    assert isinstance(result, dict)
    assert result['host'] == 'localhost'
    assert result['port'] == 5432
    assert result['user'] == 'postgres'
    assert result['admin_database'] == 'postgres'
    assert result['warehouse_database'] == 'sql_retail_analytics_warehouse'


@pytest.mark.unit
def test_test_connection_success_warehouse_exists(mock_config):
    """
    Test verify_connection when warehouse database exists.
    
    Verifies function returns success with appropriate message.
    """
    with patch('utils.database_utils.check_database_available', return_value=True), \
         patch('utils.database_utils.verify_database_exists', return_value=True):
        
        success, message = verify_connection()
        
        assert success is True
        assert 'Connected to PostgreSQL' in message
        assert 'exists' in message
        assert 'sql_retail_analytics_warehouse' in message


@pytest.mark.unit
def test_test_connection_success_warehouse_not_exists(mock_config):
    """
    Test verify_connection when warehouse database does not exist yet.
    
    Verifies function returns success with appropriate message.
    """
    with patch('utils.database_utils.check_database_available', return_value=True), \
         patch('utils.database_utils.verify_database_exists', return_value=False):
        
        success, message = verify_connection()
        
        assert success is True
        assert 'Connected to PostgreSQL' in message
        assert 'does not exist yet' in message


@pytest.mark.unit
def test_test_connection_failure_server_unavailable(mock_config):
    """
    Test verify_connection when PostgreSQL server is unavailable.
    
    Verifies function returns failure with appropriate message.
    """
    with patch('utils.database_utils.check_database_available', return_value=False):
        success, message = verify_connection()
        
        assert success is False
        assert 'not available' in message


@pytest.mark.unit
def test_test_connection_exception_handling(mock_config):
    """
    Test verify_connection handles exceptions gracefully.
    
    Verifies function returns failure message when exception occurs.
    """
    with patch('utils.database_utils.check_database_available', side_effect=Exception("Network error")):
        success, message = verify_connection()
        
        assert success is False
        assert 'Connection test failed' in message
        assert 'Network error' in message


# =======================
# 2. INTEGRATION TESTS
# =======================

@pytest.mark.integration
def test_connection_string_used_in_engine_creation(mock_config):
    """
    Test integration between get_connection_string and create_sqlalchemy_engine.
    
    Verifies connection string building integrates properly with engine creation.
    """
    with patch('utils.database_utils.URL.create', side_effect=AttributeError), \
         patch('utils.database_utils.create_engine') as mock_create_engine:
        
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Create engine which should fall back to connection string
        create_sqlalchemy_engine(database='test_db')
        
        # Verify create_engine was called with a connection string
        call_args = mock_create_engine.call_args[0]
        assert 'postgresql://' in call_args[0]
        assert 'test_db' in call_args[0]


@pytest.mark.integration
def test_wait_for_database_uses_check_database_available(mock_config):
    """
    Test wait_for_database integration with check_database_available.
    
    Verifies wait_for_database properly delegates to availability check.
    """
    check_calls = []
    
    def mock_check(*args, **kwargs):
        check_calls.append((args, kwargs))
        return len(check_calls) >= 2  # Succeed on second call
    
    with patch('utils.database_utils.check_database_available', side_effect=mock_check), \
         patch('utils.database_utils.time.sleep'):
        
        result = wait_for_database(
            host='testhost',
            port=5433,
            max_retries=5
        )
        
        assert result is True
        assert len(check_calls) == 2


@pytest.mark.integration
def test_verify_database_uses_engine_creation(mock_config):
    """
    Test verify_database_exists integration with engine creation.
    
    Verifies database verification properly creates and disposes engine.
    """
    fake_result = FakeResult(rows=[(1,)])
    fake_conn = FakeSQLAlchemyConnection(execute_result=fake_result)
    fake_engine = FakeEngine(connection=fake_conn)
    
    engine_calls = []
    
    def mock_create_engine(**kwargs):
        engine_calls.append(kwargs)
        return fake_engine
    
    with patch('utils.database_utils.create_sqlalchemy_engine', side_effect=mock_create_engine):
        result = verify_database_exists('test_db')
        
        assert result is True
        assert len(engine_calls) == 1
        assert engine_calls[0]['database'] == 'postgres'
        assert fake_engine.disposed is True


# ====================
# 3. EDGE CASE TESTS
# ====================

@pytest.mark.edge_case
def test_get_connection_string_empty_password(mock_config):
    """
    Test connection string generation with empty password.
    
    Verifies function handles empty password correctly when explicitly provided.
    Note: Empty string ('') is different from None - it should override config default.
    """
    result = get_connection_string(password='')
    
    # Empty password should be used (not the mock config's 'secret123')
    # The function should distinguish between password=None (use default) and password='' (use empty)
    assert result == "postgresql://postgres:@localhost:5432/postgres"


@pytest.mark.edge_case
def test_wait_for_database_zero_retries(mock_config):
    """
    Test wait_for_database with zero max_retries.
    
    Verifies function raises error immediately when max_retries=0.
    With range(1, 0+1) = range(1, 1), no attempts are made, so it should fail.
    """
    with patch('utils.database_utils.check_database_available', return_value=False), \
         patch('utils.database_utils.time.sleep'):
        
        # With 0 retries, the loop doesn't execute, so it should raise immediately
        with pytest.raises(DatabaseConnectionError) as exc_info:
            wait_for_database(max_retries=0)
        
        assert 'did not become available after 0 attempts' in str(exc_info.value)


@pytest.mark.edge_case
def test_check_database_available_zero_timeout(mock_config):
    """
    Test database availability check with zero timeout.
    
    Verifies function handles zero timeout.
    """
    with patch('utils.database_utils.psycopg2.connect') as mock_connect:
        mock_connect.return_value = FakeConnection()
        
        check_database_available(timeout=0)
        
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['connect_timeout'] == 0


@pytest.mark.edge_case
def test_verify_database_exists_special_chars_in_name(mock_config):
    """
    Test verify_database_exists with special characters in database name.
    
    Verifies function handles special database names safely.
    """
    fake_result = FakeResult(rows=[(1,)])
    fake_conn = FakeSQLAlchemyConnection(execute_result=fake_result)
    fake_engine = FakeEngine(connection=fake_conn)
    
    with patch('utils.database_utils.create_sqlalchemy_engine', return_value=fake_engine):
        result = verify_database_exists("test'db\"name")
        
        # Should use parameterized query (safe from injection)
        stmt, params = fake_conn.executed_statements[0]
        assert params == {'db_name': "test'db\"name"}
        assert result is True


# =================
# 4. SMOKE TESTS
# =================

@pytest.mark.smoke
def test_module_imports():
    """
    Smoke test: Verify all public functions can be imported.
    
    Ensures module structure is intact and exports are defined.
    """
    from utils.database_utils import (
        DatabaseConnectionError,
        check_database_available,
        create_sqlalchemy_engine,
        get_connection_string,
        get_database_connection_info,
        verify_connection,
        verify_database_exists,
        wait_for_database,
    )

    # Verify all imports are callable or are exception classes
    assert callable(check_database_available)
    assert callable(create_sqlalchemy_engine)
    assert callable(get_connection_string)
    assert callable(get_database_connection_info)
    assert callable(verify_connection)
    assert callable(verify_database_exists)
    assert callable(wait_for_database)
    assert issubclass(DatabaseConnectionError, Exception)


@pytest.mark.smoke
def test_get_connection_string_basic(mock_config):
    """
    Smoke test: Basic connection string generation works.
    
    Quick verification that primary use case functions.
    """
    result = get_connection_string()
    
    assert result.startswith('postgresql://')
    assert '@' in result
    assert ':' in result


@pytest.mark.smoke
def test_get_database_connection_info_basic(mock_config):
    """
    Smoke test: Connection info retrieval returns expected keys.
    
    Quick verification that config access works.
    """
    result = get_database_connection_info()
    
    required_keys = ['host', 'port', 'user', 'admin_database', 'warehouse_database']
    for key in required_keys:
        assert key in result
