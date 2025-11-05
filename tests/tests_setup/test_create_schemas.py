"""
=================================================
Comprehensive pytest suite for create_schemas.py
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
- SchemaCreator initialization
- Schema creation (create_schema, create_all_schemas)
- Individual schema methods (create_bronze_schema, create_silver_schema, etc.)
- Schema existence checking (check_schema_exists)
- Schema verification (verify_all_schemas)
- Schema information retrieval (get_schema_info)
- LoggingInfrastructure integration
- Transaction management with engine.begin()
- Error handling and exception propagation
- Resource cleanup (close_connections)

How to Execute:
---------------
All tests:          pytest tests/tests_setup/test_create_schemas.py -v
By category:        pytest tests/tests_setup/test_create_schemas.py -m unit
Multiple markers:   pytest tests/tests_setup/test_create_schemas.py -m "unit or integration"
Specific test:      pytest tests/tests_setup/test_create_schemas.py::test_create_schema_success
With coverage:      pytest tests/tests_setup/test_create_schemas.py --cov=setup.create_schemas

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.
"""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema

# ====================
# Helper Functions
# ====================

@contextmanager
def create_mock_connection():
    """
    Helper to create a proper context manager for mocking engine.begin().
    
    This ensures the connection is properly yielded and committed/rolled back.
    """
    mock_conn = MagicMock()
    mock_conn.execute = MagicMock()
    yield mock_conn


# ====================
# Mock Helper Classes
# ====================

class FakeConnection:
    """
    Mock SQLAlchemy Connection for testing DDL operations.
    
    Simulates SQLAlchemy Connection behavior with engine.begin() context manager.
    """
    
    def __init__(self, execute_results=None, execute_side_effect=None):
        self.executed_statements = []
        self.committed = False
        self.rolled_back = False
        self._execute_results = execute_results or {}
        self._execute_side_effect = execute_side_effect
    
    def execute(self, statement, parameters=None):
        """Execute a statement and return mock result."""
        # Store the executed statement for verification
        self.executed_statements.append({
            'statement': statement,
            'parameters': parameters
        })
        
        if self._execute_side_effect:
            raise self._execute_side_effect
        
        # Return appropriate mock result
        if hasattr(statement, '__class__'):
            stmt_type = statement.__class__.__name__
            if stmt_type in self._execute_results:
                return self._execute_results[stmt_type]
        
        # Default mock result
        return FakeResult()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False


class FakeResult:
    """
    Mock SQLAlchemy Result for query results.
    """
    
    def __init__(self, rows=None):
        self._rows = rows or []
        self._index = 0
    
    def fetchone(self):
        """Fetch one row."""
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        return None
    
    def fetchall(self):
        """Fetch all rows."""
        return self._rows
    
    def __iter__(self):
        """Iterator support."""
        return iter(self._rows)


class FakeEngine:
    """
    Mock SQLAlchemy Engine for testing.
    """
    
    def __init__(self, connection=None):
        self._connection = connection or FakeConnection()
        self.disposed = False
    
    def begin(self):
        """Return connection context manager."""
        return self._connection
    
    def dispose(self):
        """Dispose engine."""
        self.disposed = True


# =========================
# UNIT TESTS
# =========================

@pytest.mark.unit
def test_schema_creator_init(schema_creator_factory):
    """Test SchemaCreator initialization with default parameters."""
    creator = schema_creator_factory()
    
    assert creator.host == "localhost"
    assert creator.port == 5432
    assert creator.user == "postgres"
    assert creator.password == "secret"
    assert creator.database == "warehouse"
    assert creator._engine is None
    assert creator._metadata is None
    assert creator._logging_infra is None
    assert creator._process_log_id is None


@pytest.mark.unit
def test_schema_creator_init_custom_params(schema_creator_factory):
    """Test SchemaCreator initialization with custom parameters."""
    creator = schema_creator_factory(
        host="custom-host",
        port=5433,
        user="admin",
        password="admin123",
        database="custom_db"
    )
    
    assert creator.host == "custom-host"
    assert creator.port == 5433
    assert creator.user == "admin"
    assert creator.password == "admin123"
    assert creator.database == "custom_db"


@pytest.mark.unit
def test_schema_definitions(schema_creator_factory):
    """Test that SCHEMAS constant is properly defined."""
    creator = schema_creator_factory()
    
    assert 'bronze' in creator.SCHEMAS
    assert 'silver' in creator.SCHEMAS
    assert 'gold' in creator.SCHEMAS
    assert 'logs' in creator.SCHEMAS
    
    assert 'Raw data layer' in creator.SCHEMAS['bronze']
    assert 'Cleansed data layer' in creator.SCHEMAS['silver']
    assert 'Business data layer' in creator.SCHEMAS['gold']
    assert 'Logging infrastructure' in creator.SCHEMAS['logs']


@pytest.mark.unit
def test_get_engine_creates_engine_once(patch_schemas_create_engine, schema_creator_factory):
    """Test that _get_engine creates engine only once and caches it."""
    mock_engine = MagicMock()
    patch_schemas_create_engine.return_value = mock_engine
    
    creator = schema_creator_factory()
    
    # First call should create engine
    engine1 = creator._get_engine()
    assert engine1 is mock_engine
    assert patch_schemas_create_engine.call_count == 1
    
    # Second call should return cached engine
    engine2 = creator._get_engine()
    assert engine2 is mock_engine
    assert patch_schemas_create_engine.call_count == 1  # Still 1, not 2


@pytest.mark.unit
def test_get_engine_uses_url_create(patch_schemas_create_engine, schema_creator_factory):
    """Test that _get_engine uses URL.create() for modern SQLAlchemy."""
    mock_engine = MagicMock()
    patch_schemas_create_engine.return_value = mock_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.URL.create") as mock_url_create:
        mock_url_create.return_value = "postgresql://connection_url"
        
        engine = creator._get_engine()
        
        # Verify URL.create was called with correct parameters
        mock_url_create.assert_called_once_with(
            drivername='postgresql',
            username='postgres',
            password='secret',
            host='localhost',
            port=5432,
            database='warehouse'
        )
        
        # Verify create_engine was called with URL
        patch_schemas_create_engine.assert_called_once_with(
            "postgresql://connection_url",
            echo=False
        )


@pytest.mark.unit
def test_get_engine_fallback_to_connection_string(patch_schemas_create_engine, schema_creator_factory):
    """Test that _get_engine falls back to connection string for older SQLAlchemy."""
    mock_engine = MagicMock()
    patch_schemas_create_engine.return_value = mock_engine
    
    creator = schema_creator_factory()
    
    # Simulate older SQLAlchemy by making URL.create raise AttributeError
    with patch("setup.create_schemas.URL.create", side_effect=AttributeError):
        engine = creator._get_engine()
        
        # Verify create_engine was called with connection string
        call_args = patch_schemas_create_engine.call_args
        assert "postgresql://" in call_args[0][0]
        assert "postgres:secret@localhost:5432/warehouse" in call_args[0][0]


@pytest.mark.unit
def test_get_metadata_creates_metadata_once(schema_creator_factory):
    """Test that _get_metadata creates MetaData only once and caches it."""
    creator = schema_creator_factory()
    
    # First call should create metadata
    metadata1 = creator._get_metadata()
    assert metadata1 is not None
    
    # Second call should return cached metadata
    metadata2 = creator._get_metadata()
    assert metadata2 is metadata1


@pytest.mark.unit
def test_check_schema_exists_returns_true(patch_schemas_create_engine, schema_creator_factory):
    """Test check_schema_exists returns True when schema exists."""
    # Setup fake connection that returns a row (schema exists)
    fake_result = FakeResult(rows=[SimpleNamespace(exists=1)])
    fake_conn = FakeConnection(execute_results={'TextClause': fake_result})
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.check_schema_exists_sql", return_value="SELECT 1 FROM..."):
        exists = creator.check_schema_exists('bronze')
    
    assert exists is True


@pytest.mark.unit
def test_check_schema_exists_returns_false(patch_schemas_create_engine, schema_creator_factory):
    """Test check_schema_exists returns False when schema doesn't exist."""
    # Setup fake connection that returns no rows (schema doesn't exist)
    fake_result = FakeResult(rows=[])
    fake_conn = FakeConnection(execute_results={'TextClause': fake_result})
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.check_schema_exists_sql", return_value="SELECT 1 FROM..."):
        exists = creator.check_schema_exists('nonexistent')
    
    assert exists is False


@pytest.mark.unit
def test_check_schema_exists_raises_on_error(patch_schemas_create_engine, schema_creator_factory):
    """Test check_schema_exists raises SchemaCreationError on database error."""
    from setup.create_schemas import SchemaCreationError

    # Setup fake connection that raises error
    fake_conn = FakeConnection(execute_side_effect=SQLAlchemyError("Connection failed"))
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.check_schema_exists_sql", return_value="SELECT 1 FROM..."):
        with pytest.raises(SchemaCreationError, match="Failed to check schema existence"):
            creator.check_schema_exists('bronze')


@pytest.mark.unit
def test_create_schema_success(patch_schemas_create_engine, schema_creator_factory):
    """Test create_schema successfully creates a new schema."""
    from setup.create_schemas import SchemaCreationError
    
    fake_conn = FakeConnection()
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    # Mock check_schema_exists to return False (schema doesn't exist)
    with patch.object(creator, 'check_schema_exists', return_value=False):
        result = creator.create_schema('bronze', 'Bronze layer description')
    
    assert result is True
    assert len(fake_conn.executed_statements) == 2  # CREATE SCHEMA + COMMENT
    assert fake_conn.committed is True


@pytest.mark.unit
def test_create_schema_already_exists(patch_schemas_create_engine, schema_creator_factory):
    """Test create_schema returns False when schema already exists."""
    fake_conn = FakeConnection()
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    # Mock check_schema_exists to return True (schema already exists)
    with patch.object(creator, 'check_schema_exists', return_value=True):
        result = creator.create_schema('bronze', 'Bronze layer description')
    
    assert result is False
    assert len(fake_conn.executed_statements) == 0  # No statements executed


@pytest.mark.unit
def test_create_schema_raises_on_error(patch_schemas_create_engine, schema_creator_factory):
    """Test create_schema raises SchemaCreationError on database error."""
    from setup.create_schemas import SchemaCreationError
    
    fake_conn = FakeConnection(execute_side_effect=SQLAlchemyError("Schema creation failed"))
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=False):
        with pytest.raises(SchemaCreationError, match="Failed to create schema 'bronze'"):
            creator.create_schema('bronze', 'Bronze layer description')


@pytest.mark.unit
def test_create_bronze_schema(patch_schemas_create_engine, schema_creator_factory):
    """Test create_bronze_schema calls create_schema with correct parameters."""
    creator = schema_creator_factory()
    
    with patch.object(creator, 'create_schema', return_value=True) as mock_create:
        result = creator.create_bronze_schema()
    
    mock_create.assert_called_once_with('bronze', creator.SCHEMAS['bronze'])
    assert result is True


@pytest.mark.unit
def test_create_silver_schema(patch_schemas_create_engine, schema_creator_factory):
    """Test create_silver_schema calls create_schema with correct parameters."""
    creator = schema_creator_factory()
    
    with patch.object(creator, 'create_schema', return_value=True) as mock_create:
        result = creator.create_silver_schema()
    
    mock_create.assert_called_once_with('silver', creator.SCHEMAS['silver'])
    assert result is True


@pytest.mark.unit
def test_create_gold_schema(patch_schemas_create_engine, schema_creator_factory):
    """Test create_gold_schema calls create_schema with correct parameters."""
    creator = schema_creator_factory()
    
    with patch.object(creator, 'create_schema', return_value=True) as mock_create:
        result = creator.create_gold_schema()
    
    mock_create.assert_called_once_with('gold', creator.SCHEMAS['gold'])
    assert result is True


@pytest.mark.unit
def test_create_logs_schema(patch_schemas_create_engine, schema_creator_factory):
    """Test create_logs_schema calls create_schema with correct parameters."""
    creator = schema_creator_factory()
    
    with patch.object(creator, 'create_schema', return_value=True) as mock_create:
        result = creator.create_logs_schema()
    
    mock_create.assert_called_once_with('logs', creator.SCHEMAS['logs'])
    assert result is True


@pytest.mark.unit
def test_create_all_schemas_success(patch_schemas_create_engine, schema_creator_factory):
    """Test create_all_schemas creates all schemas in correct order."""
    creator = schema_creator_factory()
    
    # Track the order of schema creation
    schema_order = []
    
    def track_create(schema_name, description):
        schema_order.append(schema_name)
        return True
    
    with patch.object(creator, 'create_schema', side_effect=track_create):
        results = creator.create_all_schemas()
    
    # Verify all schemas were created
    assert results == {
        'logs': True,
        'bronze': True,
        'silver': True,
        'gold': True
    }
    
    # Verify correct order: logs first, then bronze, silver, gold
    assert schema_order == ['logs', 'bronze', 'silver', 'gold']


@pytest.mark.unit
def test_create_all_schemas_some_exist(patch_schemas_create_engine, schema_creator_factory):
    """Test create_all_schemas handles mix of new and existing schemas."""
    creator = schema_creator_factory()
    
    schema_results = {
        'logs': False,      # Already exists
        'bronze': True,     # Created
        'silver': False,    # Already exists
        'gold': True        # Created
    }
    
    def create_mock(schema_name, description):
        return schema_results[schema_name]
    
    with patch.object(creator, 'create_schema', side_effect=create_mock):
        results = creator.create_all_schemas()
    
    assert results == schema_results


@pytest.mark.unit
def test_create_all_schemas_raises_on_error(patch_schemas_create_engine, schema_creator_factory):
    """Test create_all_schemas raises SchemaCreationError on failure."""
    from setup.create_schemas import SchemaCreationError
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'create_schema', side_effect=SchemaCreationError("Failed")):
        with pytest.raises(SchemaCreationError):
            creator.create_all_schemas()


@pytest.mark.unit
def test_verify_all_schemas_all_exist(patch_schemas_create_engine, schema_creator_factory):
    """Test verify_all_schemas returns True for all when all schemas exist."""
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=True):
        results = creator.verify_all_schemas()
    
    assert results == {
        'bronze': True,
        'silver': True,
        'gold': True,
        'logs': True
    }


@pytest.mark.unit
def test_verify_all_schemas_some_missing(patch_schemas_create_engine, schema_creator_factory):
    """Test verify_all_schemas identifies missing schemas."""
    creator = schema_creator_factory()
    
    schema_status = {
        'bronze': True,
        'silver': False,  # Missing
        'gold': True,
        'logs': False     # Missing
    }
    
    with patch.object(creator, 'check_schema_exists', side_effect=lambda s: schema_status[s]):
        results = creator.verify_all_schemas()
    
    assert results == schema_status


@pytest.mark.unit
def test_get_schema_info_returns_info(patch_schemas_create_engine, schema_creator_factory):
    """Test get_schema_info returns schema information."""
    # Setup fake result with schema info
    fake_rows = [
        SimpleNamespace(
            schema_name='bronze',
            schema_owner='postgres',
            description='Bronze layer'
        ),
        SimpleNamespace(
            schema_name='silver',
            schema_owner='postgres',
            description='Silver layer'
        )
    ]
    
    fake_result = FakeResult(rows=fake_rows)
    fake_conn = FakeConnection(execute_results={'TextClause': fake_result})
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.get_schema_info_sql", return_value="SELECT ..."):
        info = creator.get_schema_info()
    
    assert len(info) == 2
    assert info[0]['schema_name'] == 'bronze'
    assert info[0]['schema_owner'] == 'postgres'
    assert info[0]['description'] == 'Bronze layer'


@pytest.mark.unit
def test_get_schema_info_handles_none_description(patch_schemas_create_engine, schema_creator_factory):
    """Test get_schema_info handles None description values."""
    fake_rows = [
        SimpleNamespace(
            schema_name='bronze',
            schema_owner='postgres',
            description=None
        )
    ]
    
    fake_result = FakeResult(rows=fake_rows)
    fake_conn = FakeConnection(execute_results={'TextClause': fake_result})
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.get_schema_info_sql", return_value="SELECT ..."):
        info = creator.get_schema_info()
    
    assert info[0]['description'] == 'No description'


@pytest.mark.unit
def test_get_schema_info_raises_on_error(patch_schemas_create_engine, schema_creator_factory):
    """Test get_schema_info raises SchemaCreationError on database error."""
    from setup.create_schemas import SchemaCreationError
    
    fake_conn = FakeConnection(execute_side_effect=SQLAlchemyError("Query failed"))
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch("setup.create_schemas.get_schema_info_sql", return_value="SELECT ..."):
        with pytest.raises(SchemaCreationError, match="Failed to get schema information"):
            creator.get_schema_info()


@pytest.mark.unit
def test_close_connections_disposes_engine(patch_schemas_create_engine, schema_creator_factory):
    """Test close_connections disposes of engine."""
    fake_engine = FakeEngine()
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    creator._get_engine()  # Initialize engine
    
    assert fake_engine.disposed is False
    
    creator.close_connections()
    
    assert fake_engine.disposed is True
    assert creator._engine is None


@pytest.mark.unit
def test_close_connections_closes_logging_infra(patch_schemas_create_engine, schema_creator_factory):
    """Test close_connections also closes LoggingInfrastructure."""
    fake_engine = FakeEngine()
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    # Mock logging infrastructure
    mock_logging_infra = MagicMock()
    creator._logging_infra = mock_logging_infra
    
    creator.close_connections()
    
    mock_logging_infra.close_connections.assert_called_once()
    assert creator._logging_infra is None


@pytest.mark.unit
def test_close_connections_when_no_engine(schema_creator_factory):
    """Test close_connections handles case when engine is None."""
    creator = schema_creator_factory()
    
    # Should not raise error
    creator.close_connections()
    
    assert creator._engine is None


# =========================
# INTEGRATION TESTS
# =========================

@pytest.mark.integration
def test_engine_begin_context_manager_commits_on_success(patch_schemas_create_engine, schema_creator_factory):
    """Test that engine.begin() commits transaction on success."""
    fake_conn = FakeConnection()
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=False):
        creator.create_schema('bronze', 'Bronze layer')
    
    assert fake_conn.committed is True
    assert fake_conn.rolled_back is False


@pytest.mark.integration
def test_engine_begin_context_manager_rolls_back_on_error(patch_schemas_create_engine, schema_creator_factory):
    """Test that engine.begin() rolls back transaction on error."""
    from setup.create_schemas import SchemaCreationError
    
    fake_conn = FakeConnection(execute_side_effect=SQLAlchemyError("Error"))
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=False):
        with pytest.raises(SchemaCreationError):
            creator.create_schema('bronze', 'Bronze layer')
    
    assert fake_conn.committed is False
    assert fake_conn.rolled_back is True


@pytest.mark.integration
def test_get_logging_infra_returns_none_when_logs_schema_missing(patch_schemas_create_engine, schema_creator_factory):
    """Test _get_logging_infra returns None when logs schema doesn't exist yet."""
    fake_engine = FakeEngine()
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=False):
        logging_infra = creator._get_logging_infra()
    
    assert logging_infra is None


@pytest.mark.integration
def test_get_logging_infra_initializes_when_logs_schema_exists(patch_schemas_create_engine, schema_creator_factory):
    """Test _get_logging_infra initializes LoggingInfrastructure when logs schema exists."""
    fake_engine = FakeEngine()
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    with patch.object(creator, 'check_schema_exists', return_value=True):
        with patch("setup.create_schemas.LoggingInfrastructure") as mock_logging_class:
            mock_logging_instance = MagicMock()
            mock_logging_class.return_value = mock_logging_instance
            
            logging_infra = creator._get_logging_infra()
            
            assert logging_infra is mock_logging_instance
            mock_logging_class.assert_called_once_with(
                host='localhost',
                port=5432,
                user='postgres',
                password='secret',
                database='warehouse'
            )


@pytest.mark.integration
def test_process_logging_integration(patch_schemas_create_engine, schema_creator_factory):
    """Test that process logging is called when available."""
    fake_conn = FakeConnection()
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    # Mock logging infrastructure
    mock_logging_infra = MagicMock()
    mock_logging_infra.log_process_start.return_value = 123
    
    with patch.object(creator, '_get_logging_infra', return_value=mock_logging_infra):
        with patch.object(creator, 'check_schema_exists', return_value=False):
            creator.create_schema('bronze', 'Bronze layer')
    
    # Verify process logging was called
    mock_logging_infra.log_process_start.assert_called_once()
    mock_logging_infra.log_process_end.assert_called_once_with(
        log_id=123,
        status='SUCCESS',
        error_message=None
    )


@pytest.mark.integration
def test_process_logging_logs_failure(patch_schemas_create_engine, schema_creator_factory):
    """Test that process logging logs failure status on error."""
    from setup.create_schemas import SchemaCreationError
    
    fake_conn = FakeConnection(execute_side_effect=SQLAlchemyError("Schema error"))
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    # Mock logging infrastructure
    mock_logging_infra = MagicMock()
    mock_logging_infra.log_process_start.return_value = 123
    
    with patch.object(creator, '_get_logging_infra', return_value=mock_logging_infra):
        with patch.object(creator, 'check_schema_exists', return_value=False):
            with pytest.raises(SchemaCreationError):
                creator.create_schema('bronze', 'Bronze layer')
    
    # Verify process logging logged the failure
    mock_logging_infra.log_process_end.assert_called_once()
    call_args = mock_logging_infra.log_process_end.call_args
    assert call_args[1]['status'] == 'FAILED'
    assert 'Schema error' in call_args[1]['error_message']


# =========================
# EDGE CASE TESTS
# =========================

@pytest.mark.edge_case
def test_create_schema_with_special_characters_in_description(patch_schemas_create_engine, schema_creator_factory):
    """Test create_schema handles special characters in description."""
    fake_conn = FakeConnection()
    fake_engine = FakeEngine(connection=fake_conn)
    
    patch_schemas_create_engine.return_value = fake_engine
    
    creator = schema_creator_factory()
    
    special_desc = "Layer with 'quotes' and \"double quotes\" and \n newlines"
    
    with patch.object(creator, 'check_schema_exists', return_value=False):
        result = creator.create_schema('bronze', special_desc)
    
    assert result is True
    # Verify description was passed as parameter
    comment_stmt = fake_conn.executed_statements[1]
    assert comment_stmt['parameters']['description'] == special_desc


@pytest.mark.edge_case
def test_create_all_schemas_handles_partial_failure(patch_schemas_create_engine, schema_creator_factory):
    """Test create_all_schemas stops on first failure."""
    from setup.create_schemas import SchemaCreationError
    
    creator = schema_creator_factory()
    
    call_count = [0]
    
    def create_mock(schema_name, description):
        call_count[0] += 1
        if schema_name == 'bronze':
            raise SchemaCreationError("Failed to create bronze")
        return True
    
    with patch.object(creator, 'create_schema', side_effect=create_mock):
        with pytest.raises(SchemaCreationError, match="Failed to create bronze"):
            creator.create_all_schemas()
    
    # Should have attempted logs and bronze only (bronze failed)
    assert call_count[0] == 2


@pytest.mark.edge_case
def test_verify_all_schemas_with_empty_schemas_dict(schema_creator_factory):
    """Test verify_all_schemas handles empty SCHEMAS dict."""
    creator = schema_creator_factory()
    
    # Temporarily empty the SCHEMAS dict
    original_schemas = creator.SCHEMAS.copy()
    creator.SCHEMAS = {}
    
    try:
        results = creator.verify_all_schemas()
        assert results == {}
    finally:
        creator.SCHEMAS = original_schemas


# =========================
# SMOKE TESTS
# =========================

@pytest.mark.smoke
def test_schema_creator_can_be_imported():
    """Smoke test: Verify SchemaCreator can be imported."""
    from setup.create_schemas import SchemaCreator
    assert SchemaCreator is not None


@pytest.mark.smoke
def test_schema_creator_can_be_instantiated():
    """Smoke test: Verify SchemaCreator can be instantiated."""
    from setup.create_schemas import SchemaCreator
    
    creator = SchemaCreator(
        host='localhost',
        port=5432,
        user='postgres',
        password='secret',
        database='warehouse'
    )
    
    assert creator is not None
    assert isinstance(creator, SchemaCreator)


@pytest.mark.smoke
def test_schema_creation_error_can_be_raised():
    """Smoke test: Verify SchemaCreationError can be raised."""
    from setup.create_schemas import SchemaCreationError
    
    with pytest.raises(SchemaCreationError, match="Test error"):
        raise SchemaCreationError("Test error")


@pytest.mark.smoke
def test_schema_creator_has_all_required_methods():
    """Smoke test: Verify SchemaCreator has all required methods."""
    from setup.create_schemas import SchemaCreator
    
    creator = SchemaCreator(
        host='localhost',
        port=5432,
        user='postgres',
        password='secret',
        database='warehouse'
    )
    
    required_methods = [
        '_get_engine',
        '_get_metadata',
        '_get_logging_infra',
        '_start_process_logging',
        '_end_process_logging',
        'check_schema_exists',
        'create_schema',
        'create_bronze_schema',
        'create_silver_schema',
        'create_gold_schema',
        'create_logs_schema',
        'create_all_schemas',
        'verify_all_schemas',
        'get_schema_info',
        'close_connections'
    ]
    
    for method in required_methods:
        assert hasattr(creator, method), f"Missing method: {method}"
        assert callable(getattr(creator, method)), f"Not callable: {method}"


@pytest.mark.smoke
def test_logging_available_flag():
    """Smoke test: Verify LOGGING_AVAILABLE flag is set correctly."""
    from setup.create_schemas import LOGGING_AVAILABLE

    # Should be True since LoggingInfrastructure is available in our test environment
    assert isinstance(LOGGING_AVAILABLE, bool)
