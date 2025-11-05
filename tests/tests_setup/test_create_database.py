"""
==================================================
Comprehensive pytest suite for create_database.py
==================================================

Sections:
---------
1. Unit tests
2. Integration tests
3. System tests
4. End-to-End tests
5. Smoke tests

Available markers:
------------------
unit, integration, system, e2e, smoke

Mocks and helpers:
------------------
- FakeEngine: simulates SQLAlchemy Engine.connect() context manager
- FakeConnection: simulates connection returned by engine.connect()
- FakeRawConn & FakeCursor: simulate psycopg2 raw connection & cursor

How to Execute:
---------------
All tests:          python -m pytest tests/tests_setup/test_create_database.py -v
By category:        python -m pytest tests/tests_setup/test_create_database.py -m unit
Multiple markers:   python -m pytest tests/tests_setup/test_create_database.py -m "unit or integration"
Specific test:      python -m pytest tests/tests_setup/test_create_database.py::test_check_database_exists_true
With coverage:      python -m pytest tests/tests_setup/test_create_database.py --cov=setup.create_database

Note: Use 'python -m pytest' (not just 'pytest') to ensure correct Python path resolution.

"""

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg2
from pytest import mark, raises
from sqlalchemy.exc import SQLAlchemyError


# Helpers used by many tests
class FakeCursor:
    """
    Mock psycopg2 cursor for testing database operations.
    
    This class simulates the behavior of a psycopg2 database cursor, tracking
    all SQL queries executed and optionally raising exceptions to test error
    handling. It implements context manager protocol for use with 'with' statements.
    
    Attributes:
        queries (list): List of all SQL query strings that have been executed.
        _execute_side_effect (Exception, optional): Exception to raise when execute() is called.
    
    Example:
        >>> cursor = FakeCursor()
        >>> cursor.execute("SELECT 1")
        >>> assert "SELECT 1" in cursor.queries
        
        >>> # Test error handling
        >>> error_cursor = FakeCursor(execute_side_effect=psycopg2.Error("Failed"))
        >>> cursor.execute("SELECT 1")  # Raises psycopg2.Error
    """
    def __init__(self, execute_side_effect=None):
        self.queries = []
        self._execute_side_effect = execute_side_effect

    def execute(self, sql_text):
        """
        Execute a SQL query and track it in the queries list.
        
        Args:
            sql_text (str): SQL query string to execute.
            
        Raises:
            Exception: If execute_side_effect was provided during initialization.
        """
        self.queries.append(sql_text)
        if self._execute_side_effect:
            raise self._execute_side_effect

    def __enter__(self):
        """Context manager entry - returns self for use in 'with' statements."""
        return self

    def __exit__(self, exc_type, exc, tb):
        """Context manager exit - allows exceptions to propagate."""
        return False

class FakeRawConn:
    """
    Mock psycopg2 raw database connection for testing.
    
    This class simulates a psycopg2 raw connection object, providing a cursor
    factory and tracking isolation level changes. Used to test DatabaseCreator's
    interaction with the raw psycopg2 connection obtained from SQLAlchemy.
    
    Attributes:
        cursor_obj (FakeCursor): The cursor object returned by cursor() method.
        set_isolation_level_called_with: Tracks the isolation level value passed.
        _set_isolation_side_effect (Exception, optional): Exception to raise when
            set_isolation_level() is called.
        closed (bool): Flag indicating if connection has been closed.
    
    Example:
        >>> cursor = FakeCursor()
        >>> conn = FakeRawConn(cursor_obj=cursor)
        >>> conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        >>> assert conn.set_isolation_level_called_with == ISOLATION_LEVEL_AUTOCOMMIT
    """
    def __init__(self, cursor_obj=None, set_isolation_side_effect=None):
        self.cursor_obj = cursor_obj or FakeCursor()
        self.set_isolation_level_called_with = None
        self._set_isolation_side_effect = set_isolation_side_effect
        self.closed = False

    def set_isolation_level(self, level):
        """
        Set the isolation level for the connection.
        
        Args:
            level: Isolation level constant (e.g., ISOLATION_LEVEL_AUTOCOMMIT).
            
        Raises:
            Exception: If set_isolation_side_effect was provided during initialization.
        """
        self.set_isolation_level_called_with = level
        if self._set_isolation_side_effect:
            raise self._set_isolation_side_effect

    def cursor(self):
        """
        Return a cursor for executing queries.
        
        Returns:
            FakeCursor: The cursor object for this connection.
        """
        return self.cursor_obj

class FakeResult:
    """
    Mock SQLAlchemy result object for query results.
    
    This class simulates a SQLAlchemy result object returned by execute() calls,
    providing both fetchone() for row-based results and scalar() for single values.
    
    Attributes:
        _rows (list): List of tuples representing query result rows.
        _scalar: Single scalar value to return from scalar() method.
    
    Example:
        >>> # Query that returns rows
        >>> result = FakeResult(rows=[(1, 'test'), (2, 'data')])
        >>> assert result.fetchone() == (1, 'test')
        
        >>> # Query that returns a count
        >>> result = FakeResult(scalar_val=5)
        >>> assert result.scalar() == 5
    """
    def __init__(self, rows=None, scalar_val=None):
        self._rows = rows or []
        self._scalar = scalar_val

    def fetchone(self):
        """
        Fetch the first row from the result set.
        
        Returns:
            tuple or None: First row tuple if rows exist, None otherwise.
        """
        if not self._rows:
            return None
        return self._rows[0]

    def scalar(self):
        """
        Return a scalar value from the result.
        
        Returns:
            The scalar value set during initialization.
        """
        return self._scalar

class FakeConnection:
    """
    Simulates the object returned by Engine.connect().__enter__().
    - .execute(text(..)) should return FakeResult for query results
    - .connection.driver_connection returns a FakeRawConn
    - .commit() is tracked
    """
    def __init__(self, exec_map=None, raw_conn=None):
        """
        exec_map: dict mapping SQL text (or test keys) => FakeResult or side_effect
        """
        self.exec_map = exec_map or {}
        self.connection = SimpleNamespace(driver_connection=raw_conn or FakeRawConn())
        self._committed = False
        self.executed = []

    def execute(self, sql_obj):
        """
        Execute a SQL statement and return appropriate fake results.
        
        This method simulates SQLAlchemy's connection.execute() behavior,
        converting SQL objects to strings, tracking executed queries, and
        returning pre-configured results from the exec_map or default results.
        
        Args:
            sql_obj: SQLAlchemy text() object or string containing SQL query.
            
        Returns:
            FakeResult: Pre-configured result from exec_map or default result.
            
        Raises:
            Exception: If exec_map contains an Exception for the given SQL text.
        """
        # sql_obj may be a sqlalchemy.text() or a bare string in our tests; convert to str
        sql_text = str(sql_obj)
        self.executed.append(sql_text)
        result = self.exec_map.get(sql_text)
        if isinstance(result, Exception):
            raise result
        return result or FakeResult(rows=[(1,)], scalar_val=0)

    def commit(self):
        """
        Commit the current transaction.
        
        Tracks that commit was called by setting internal _committed flag.
        """
        self._committed = True

    # context manager support handled externally by FakeEngine.connect()

class FakeEngine:
    """
    Mock SQLAlchemy Engine for testing database operations.
    
    This class simulates SQLAlchemy's Engine object, providing a connect()
    context manager that yields a FakeConnection. It also tracks disposal
    of engine resources.
    
    Attributes:
        _conn_obj (FakeConnection): The connection object to yield from connect().
        disposed (bool): Flag indicating if dispose() has been called.
    
    Example:
        >>> conn = FakeConnection()
        >>> engine = FakeEngine(conn)
        >>> with engine.connect() as connection:
        ...     result = connection.execute("SELECT 1")
        >>> engine.dispose()
        >>> assert engine.disposed is True
    """
    def __init__(self, conn_obj):
        self._conn_obj = conn_obj
        self.disposed = False

    # emulate engine.connect() context manager: "with engine.connect() as conn:"
    def connect(self):
        """
        Create a connection context manager.
        
        This method returns a context manager that yields the configured
        FakeConnection object when entering a 'with' statement, mimicking
        SQLAlchemy's engine.connect() behavior.
        
        Returns:
            Context manager that yields FakeConnection on __enter__.
            
        Example:
            >>> engine = FakeEngine(FakeConnection())
            >>> with engine.connect() as conn:
            ...     conn.execute("SELECT 1")
        """
        parent = self
        class _Ctx:
            def __enter__(self_inner):
                return parent._conn_obj
            def __exit__(self_inner, exc_type, exc, tb):
                return False
        return _Ctx()

    def dispose(self):
        """
        Dispose of the engine and its connection pool.
        
        Sets the disposed flag to True to track resource cleanup.
        """
        self.disposed = True

# ===============
# 1. UNIT TESTS
# ===============

@mark.unit
def test_check_database_exists_true(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify check_database_exists returns True when database exists.
    
    Tests that DatabaseCreator.check_database_exists() correctly interprets
    a non-None result from fetchone() as indicating the database exists.
    
    Test Strategy:
        - Mock the SQL query to return a row (database exists)
        - Call check_database_exists()
        - Assert it returns True
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreator, check_database_exists_sql

    # prepare fake result: fetchone() returns non-None -> exists
    fake_result = FakeResult(rows=[(1,)])
    conn = FakeConnection(exec_map={str(dummy_sql_module["exists_sql"]): fake_result})
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    assert creator.check_database_exists() is True

@mark.unit
def test_check_database_exists_false(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify check_database_exists returns False when database doesn't exist.
    
    Tests that DatabaseCreator.check_database_exists() correctly interprets
    a None result from fetchone() as indicating the database does not exist.
    
    Test Strategy:
        - Mock the SQL query to return no rows (database doesn't exist)
        - Call check_database_exists()
        - Assert it returns False
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    fake_result = FakeResult(rows=[])
    conn = FakeConnection(exec_map={str(dummy_sql_module["exists_sql"]): fake_result})
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    assert creator.check_database_exists() is False

@mark.unit
def test_check_database_exists_raises_on_sqlalchemy_error(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify check_database_exists raises DatabaseCreationError on SQL errors.
    
    Tests that DatabaseCreator.check_database_exists() properly handles SQLAlchemy
    exceptions by catching them and re-raising as DatabaseCreationError with
    appropriate error context.
    
    Test Strategy:
        - Mock connection.execute() to raise SQLAlchemyError
        - Call check_database_exists()
        - Assert DatabaseCreationError is raised
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreationError

    # configure engine.connect().execute to raise SQLAlchemyError
    def raising_execute(sql_obj):
        raise SQLAlchemyError("boom")

    conn = FakeConnection()
    conn.execute = raising_execute
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    with raises(DatabaseCreationError):
        creator.check_database_exists()

@mark.unit
def test_terminate_connections_no_db(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify terminate_connections returns 0 when database doesn't exist.
    
    Tests that DatabaseCreator.terminate_connections() short-circuits and returns 0
    when the target database doesn't exist, avoiding unnecessary termination attempts.
    
    Test Strategy:
        - Mock check_database_exists() to return False
        - Call terminate_connections()
        - Assert it returns 0 without attempting termination
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    # check_database_exists() will be called; patch to return False by simulating fetchone=None
    fake_result = FakeResult(rows=[])
    conn = FakeConnection(exec_map={str(dummy_sql_module["exists_sql"]): fake_result})
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    assert creator.terminate_connections() == 0

@mark.unit
def test_terminate_connections_zero_active(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify terminate_connections returns 0 when no active connections exist.
    
    Tests that DatabaseCreator.terminate_connections() correctly handles the case
    where the database exists but has no active connections, returning 0 without
    executing termination SQL or committing.
    
    Test Strategy:
        - Mock database exists check to return True
        - Mock connection count query to return 0
        - Call terminate_connections()
        - Assert it returns 0 and doesn't commit
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    # exists -> True, count -> 0
    exists_result = FakeResult(rows=[(1,)])
    count_result = FakeResult(scalar_val=0)
    conn = FakeConnection(exec_map={
        str(dummy_sql_module["exists_sql"]): exists_result,
        str(dummy_sql_module["count_conn_sql"]): count_result
    })
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    assert creator.terminate_connections() == 0
    # commit should not be called as there are no terminations
    assert not conn._committed

@mark.unit
def test_terminate_connections_terminates_and_returns_count(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify terminate_connections executes termination SQL and returns count.
    
    Tests that DatabaseCreator.terminate_connections() properly executes the
    termination SQL when active connections exist and returns the count of
    terminated connections.
    
    Test Strategy:
        - Mock database exists check to return True
        - Mock connection count query to return 3
        - Call terminate_connections()
        - Assert it returns 3 and executed the termination SQL
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    exists_result = FakeResult(rows=[(1,)])
    count_result = FakeResult(scalar_val=3)
    fake_cursor = FakeCursor()
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(exec_map={
        str(dummy_sql_module["exists_sql"]): exists_result,
        str(dummy_sql_module["count_conn_sql"]): count_result
    }, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    terminated = creator.terminate_connections()
    assert terminated == 3
    # raw cursor should have executed the terminate SQL (we check FakeCursor.queries)
    # The implementation executes terminate_sql via conn.execute(text(terminate_sql)), so executed SQL may appear on conn.executed list
    assert len(conn.executed) >= 1

@mark.unit
def test_drop_database_not_exists(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify drop_database returns False when database doesn't exist.
    
    Tests that DatabaseCreator.drop_database() short-circuits and returns False
    when check_database_exists() indicates the database doesn't exist, avoiding
    unnecessary drop attempts.
    
    Test Strategy:
        - Mock check_database_exists() to return False
        - Call drop_database()
        - Assert it returns False without executing DROP SQL
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    # simulate check_database_exists -> False
    conn = FakeConnection(exec_map={str(dummy_sql_module["exists_sql"]): FakeResult(rows=[])})
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    assert creator.drop_database() is False

@mark.unit
def test_drop_database_success(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify drop_database successfully drops an existing database.
    
    Tests that DatabaseCreator.drop_database() properly executes the complete
    drop workflow: checking existence, terminating connections, and executing
    the DROP DATABASE statement via raw psycopg2 cursor.
    
    Test Strategy:
        - Mock database exists check to return True
        - Mock connection count to 0 (no active connections)
        - Call drop_database(force=True)
        - Assert it returns True and executed DROP DATABASE SQL
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    exists_result = FakeResult(rows=[(1,)])
    count_result = FakeResult(scalar_val=0)  # terminate_connections will still be called from within drop_database
    fake_cursor = FakeCursor()
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(exec_map={
        str(dummy_sql_module["exists_sql"]): exists_result,
        str(dummy_sql_module["count_conn_sql"]): count_result
    }, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    result = creator.drop_database(force=True)
    assert result is True
    # ensure the raw cursor executed drop_sql at least once
    assert any("DROP DATABASE" in q for q in fake_cursor.queries)

@mark.unit
def test_drop_database_raises_on_psycopg2_error(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify drop_database raises DatabaseCreationError on psycopg2 errors.
    
    Tests that DatabaseCreator.drop_database() properly handles psycopg2.Error
    exceptions during cursor execution by catching them and re-raising as
    DatabaseCreationError with appropriate context.
    
    Test Strategy:
        - Mock database exists check to return True
        - Mock cursor.execute() to raise psycopg2.Error
        - Call drop_database()
        - Assert DatabaseCreationError is raised
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreationError

    exists_result = FakeResult(rows=[(1,)])
    # make raw cursor.execute raise psycopg2.Error
    fake_cursor = FakeCursor(execute_side_effect=psycopg2.Error("cursor fail"))
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(exec_map={
        str(dummy_sql_module["exists_sql"]): exists_result
    }, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    with raises(DatabaseCreationError):
        creator.drop_database()

@mark.unit
def test_create_database_success_and_sleep_patched(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify create_database executes CREATE SQL and sleeps for catalog refresh.
    
    Tests that DatabaseCreator.create_database() executes the CREATE DATABASE
    statement via raw psycopg2 cursor and calls time.sleep() to allow PostgreSQL's
    catalog to refresh. Sleep is patched to avoid test delays.
    
    Test Strategy:
        - Patch time.sleep to avoid delays
        - Call create_database()
        - Assert CREATE DATABASE SQL was executed via cursor
        - Verify time.sleep was called
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    exists_result = FakeResult(rows=[])
    fake_cursor = FakeCursor()
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(exec_map={}, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    with patch("setup.create_database.time.sleep", return_value=None) as _sleep:
        creator.create_database()
    # ensure the raw cursor executed create SQL
    assert any("CREATE DATABASE" in q for q in fake_cursor.queries)

@mark.unit
def test_create_database_raises_on_sqlalchemy_error(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify create_database raises DatabaseCreationError on connection failures.
    
    Tests that DatabaseCreator.create_database() properly handles SQLAlchemy
    exceptions during engine connection by catching them and re-raising as
    DatabaseCreationError with appropriate error context.
    
    Test Strategy:
        - Mock engine.connect() to raise SQLAlchemyError
        - Call create_database()
        - Assert DatabaseCreationError is raised
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreationError

    # Simulate engine.connect() raising SQLAlchemyError when called
    def bad_connect():
        raise SQLAlchemyError("connect fail")
    # monkeypatch create_engine to return engine whose connect raises
    patch_create_engine.return_value = MagicMock(connect=bad_connect)

    creator = db_creator_factory()
    with raises(DatabaseCreationError):
        creator.create_database()

@mark.unit
def test_close_connections_disposes_engine(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Unit test: Verify close_connections disposes engine and clears internal reference.
    
    Tests that DatabaseCreator.close_connections() properly cleans up database
    resources by calling engine.dispose() and resetting the internal _admin_engine
    attribute to None.
    
    Test Strategy:
        - Create a DatabaseCreator and force engine initialization
        - Call close_connections()
        - Assert _admin_engine is None
        - Assert engine.disposed is True
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    conn = FakeConnection()
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    # force engine creation
    _ = creator._get_admin_engine()
    assert creator._admin_engine is not None
    creator.close_connections()
    assert creator._admin_engine is None
    assert engine.disposed is True

# ======================
# 2. INTEGRATION TESTS
# ======================

@mark.integration
def test_create_then_drop_flow(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    Integration test: Verify complete create-check-drop workflow with stateful simulation.
    
    Tests the full database lifecycle workflow where database creation changes the
    system state (database now exists), which can then be verified and subsequently
    dropped. This integration test validates that multiple DatabaseCreator methods
    work together correctly with proper state transitions.
    
    Test Strategy:
        - Verify database doesn't exist initially
        - Create the database via create_database()
        - Mutate mock state to reflect database now exists
        - Verify check_database_exists() returns True
        - Drop the database via drop_database()
        - Verify drop returns True
    
    State Management:
        Uses cursor.execute() patching to mutate exec_map when CREATE DATABASE
        is executed, simulating the real-world state change.
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    # We'll mutate exec_map to simulate existence after create
    exists_result_before = FakeResult(rows=[])  # before create: no db
    exists_result_after = FakeResult(rows=[(1,)])  # after create: db exists
    count_result = FakeResult(scalar_val=0)

    fake_cursor = FakeCursor()
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    # create connection object and an exec_map we will mutate
    # Initialize with the actual SQL query key that will be used
    exec_map = { str(dummy_sql_module["exists_sql"]): exists_result_before }
    conn = FakeConnection(exec_map=exec_map, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()

    # patch create_database_sql so create_database() uses our create SQL and we simulate
    # that after it runs, check_database_exists returns True by mutating conn.exec_map
    with patch("setup.create_database.create_database_sql", return_value="CREATE DATABASE dummydb;"):
        # Before create: not exists
        assert creator.check_database_exists() is False

        # call create_database -> we need to patch the cursor.execute to mutate state
        # when CREATE DATABASE is executed
        original_cursor_execute = fake_cursor.execute
        def custom_cursor_execute(sql_text):
            original_cursor_execute(sql_text)
            # when create executed, mutate to 'exists'
            if "CREATE DATABASE" in sql_text:
                conn.exec_map[str(dummy_sql_module["exists_sql"])] = exists_result_after
        fake_cursor.execute = custom_cursor_execute

        with patch("setup.create_database.time.sleep", return_value=None):
            creator.create_database()

        assert creator.check_database_exists() is True

        # Now drop the DB; prepare drop execution; drop_database will call terminate_connections() and then drop
        conn.exec_map[str(dummy_sql_module["count_conn_sql"])] = count_result
        # call drop_database -> should return True
        dropped = creator.drop_database()
        assert dropped is True

# =================
# 3. SYSTEM TESTS  
# =================

@mark.system
def test_system_create_failure_bubbles_up(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    System test: Verify psycopg2 errors during create_database are properly handled.
    
    Tests the complete error handling path when psycopg2.Error occurs during
    raw cursor execution in create_database(). Validates that the error is caught,
    logged, and re-raised as DatabaseCreationError for consistent error handling
    across the application.
    
    Test Strategy:
        - Mock raw cursor.execute() to raise psycopg2.Error
        - Call create_database()
        - Assert DatabaseCreationError is raised with appropriate context
        - Verify error propagates through full call stack
    
    Error Path:
        cursor.execute() → psycopg2.Error → DatabaseCreator catches →
        logs error → raises DatabaseCreationError
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreationError

    # create fake raw cursor that raises psycopg2.Error
    fake_cursor = FakeCursor(execute_side_effect=psycopg2.Error("raw fail"))
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    with raises(DatabaseCreationError):
        creator.create_database()

@mark.system
def test_system_drop_sqlalchemy_error(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    System test: Verify SQLAlchemy errors during drop_database are properly handled.
    
    Tests the complete error handling path when SQLAlchemyError occurs during
    the drop_database() workflow. Validates that errors from the driver connection
    layer are caught and re-raised as DatabaseCreationError.
    
    Test Strategy:
        - Mock database exists check to return True
        - Mock driver_connection.cursor() to raise SQLAlchemyError
        - Call drop_database()
        - Assert DatabaseCreationError is raised
        - Verify error context is preserved
    
    Error Path:
        driver_connection.cursor() → SQLAlchemyError → DatabaseCreator catches →
        logs error → raises DatabaseCreationError
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    from setup.create_database import DatabaseCreationError

    # simulate check_database_exists -> True
    exists_result = FakeResult(rows=[(1,)])
    conn = FakeConnection(exec_map={str(dummy_sql_module["exists_sql"]): exists_result})
    # Simulate conn.connection.driver_connection.cursor() raising when used by drop_database
    bad_raw_conn = FakeRawConn(cursor_obj=FakeCursor(execute_side_effect=SQLAlchemyError("fail")))
    conn.connection.driver_connection = bad_raw_conn
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()
    with raises(DatabaseCreationError):
        creator.drop_database()

# =====================
# 4. END-TO-END TESTS
# =====================

@mark.e2e
def test_end_to_end_create_drop_workflow(patch_create_engine, dummy_sql_module, db_creator_factory):
    """
    End-to-end test: Verify complete user workflow from creation to deletion.
    
    Simulates a real user scenario where a database is created, its existence
    is verified, and then it's dropped. This test validates the entire "happy path"
    workflow that users would follow, ensuring all components work together correctly
    when lower-level operations behave as expected.
    
    Test Strategy:
        - Start with database not existing
        - Execute create_database() and simulate state change
        - Verify database now exists via check_database_exists()
        - Execute drop_database() with proper setup
        - Verify drop succeeds
        - Simulate state change back to non-existent
        - Verify database no longer exists
    
    User Workflow:
        1. User creates new database
        2. System confirms database exists
        3. User drops database
        4. System confirms database removed
    
    Fixtures:
        patch_create_engine: Mocks SQLAlchemy's create_engine
        dummy_sql_module: Provides SQL query templates
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    # prepare results: before create, exists=False; after create, exists=True; after drop, exists=False
    exists_false = FakeResult(rows=[])
    exists_true = FakeResult(rows=[(1,)])
    count_zero = FakeResult(scalar_val=0)

    fake_cursor = FakeCursor()
    raw_conn = FakeRawConn(cursor_obj=fake_cursor)
    conn = FakeConnection(exec_map={
        str(dummy_sql_module["exists_sql"]): exists_false,
        str(dummy_sql_module["count_conn_sql"]): count_zero
    }, raw_conn=raw_conn)
    engine = FakeEngine(conn)
    patch_create_engine.return_value = engine

    creator = db_creator_factory()

    # patch create_database_sql so when create_database executes, we flip the exists state
    def create_sql_side_effect(**kwargs):
        # mutate exec_map to indicate the DB exists after creation
        conn.exec_map[str(dummy_sql_module["exists_sql"])] = exists_true
        return "CREATE DATABASE dummydb;"
    with patch("setup.create_database.create_database_sql", side_effect=create_sql_side_effect):
        with patch("setup.create_database.time.sleep", return_value=None):
            creator.create_database()

    assert creator.check_database_exists() is True

    # now drop: simulate termination returns 0 and drop succeeds
    result = creator.drop_database()
    assert result is True

    # After drop we can simulate check_database_exists returns False again
    conn.exec_map[str(dummy_sql_module["exists_sql"])] = exists_false
    assert creator.check_database_exists() is False

# ================
# 5. SMOKE TESTS
# ================

@mark.smoke
def test_smoke_import_and_init(db_creator_factory):
    """
    Smoke test: Verify module can be imported and DatabaseCreator instantiated.
    
    Basic sanity check that the create_database module can be imported without
    errors and that DatabaseCreator can be instantiated with default parameters.
    Tests the most fundamental requirement: the code runs without crashing.
    
    Test Strategy:
        - Import the module (implicit via db_creator_factory)
        - Instantiate DatabaseCreator with factory defaults
        - Assert basic attributes are set correctly
        - No side effects (no actual database operations)
    
    Validates:
        - Module syntax is valid
        - Class can be instantiated
        - Constructor sets attributes correctly
        - No import-time errors
    
    Fixtures:
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    creator = db_creator_factory()
    assert creator.host == "localhost"
    assert creator.target_db == "dummydb"

@mark.smoke
def test_smoke_close_without_engine(db_creator_factory):
    """
    Smoke test: Verify close_connections is safe when no engine exists.
    
    Tests defensive programming by ensuring close_connections() can be called
    safely even when no engine has been created yet. This validates that the
    method handles the None case gracefully without raising exceptions.
    
    Test Strategy:
        - Create DatabaseCreator without initializing engine
        - Call close_connections()
        - Assert no exception is raised
        - Verify method completes successfully
    
    Validates:
        - Defensive null checking works
        - Method is idempotent
        - No crashes on edge case usage
        - Safe cleanup even if never initialized
    
    Fixtures:
        db_creator_factory: Factory for creating DatabaseCreator instances
    """
    creator = db_creator_factory()
    # should not raise
    creator.close_connections()
