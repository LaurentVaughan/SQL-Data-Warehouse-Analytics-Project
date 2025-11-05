"""
Comprehensive test suite for the data_lineage module.

Tests cover:
- LineageTracker: log_lineage, log_table_lineage, log_column_lineage
- LineageAnalyzer: get_upstream_lineage, get_downstream_lineage, get_medallion_flow
- ImpactAnalyzer: analyze_impact, _generate_recommendations  
- Session management and context managers
- Integration scenarios
- Edge cases and smoke tests
"""

from unittest.mock import MagicMock, Mock, call, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from logs.data_lineage import (
    DataLineageError,
    ImpactAnalyzer,
    LineageAnalyzer,
    LineageTracker,
)
from models.logs_models import DataLineage

# ============================================================================
# UNIT TESTS - LineageTracker
# ============================================================================


def test_lineage_tracker_initialization():
    """Test LineageTracker initializes with correct parameters."""
    tracker = LineageTracker(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert tracker.host == "test_host"
    assert tracker.port == 5555
    assert tracker.database == "test_db"
    assert tracker.user == "test_user"
    assert tracker.password == "test_pass"


def test_get_session_context_manager():
    """Test _get_session returns context manager that commits on success."""
    tracker = LineageTracker()
    mock_engine = Mock()
    mock_session = Mock(spec=Session)
    
    # Mock sessionmaker
    tracker._session_factory = Mock(return_value=mock_session)
    
    with tracker._get_session() as session:
        assert session is mock_session
    
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def test_get_session_rollback_on_exception():
    """Test _get_session rolls back transaction on exception."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    with pytest.raises(ValueError):
        with tracker._get_session() as session:
            raise ValueError("Test error")
    
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_log_lineage_success():
    """Test log_lineage successfully creates lineage record."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    # Mock added lineage record
    def mock_flush():
        # Simulate DB assigning ID
        for call_args in mock_session.add.call_args_list:
            added_obj = call_args[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = 789
    
    mock_session.flush.side_effect = mock_flush
    
    lineage_id = tracker.log_lineage(
        process_log_id=1,
        source_schema="bronze",
        source_table="raw_data",
        target_schema="silver",
        target_table="clean_data",
        transformation_logic="Cleansing",
        record_count=1000,
    )
    
    # Verify lineage was added
    assert mock_session.add.called
    added_lineage = mock_session.add.call_args[0][0]
    assert isinstance(added_lineage, DataLineage)
    assert added_lineage.process_log_id == 1
    assert added_lineage.source_schema == "bronze"
    assert added_lineage.source_table == "raw_data"
    assert added_lineage.target_schema == "silver"
    assert added_lineage.target_table == "clean_data"
    assert added_lineage.transformation_logic == "Cleansing"
    assert added_lineage.record_count == 1000
    assert lineage_id == 789


def test_log_lineage_with_column_level():
    """Test log_lineage with column-level lineage."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    def mock_flush():
        for call_args in mock_session.add.call_args_list:
            added_obj = call_args[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = 123
    
    mock_session.flush.side_effect = mock_flush
    
    lineage_id = tracker.log_lineage(
        process_log_id=1,
        source_schema="bronze",
        source_table="source",
        source_column="col_a",
        target_schema="silver",
        target_table="target",
        target_column="col_b",
        transformation_logic="UPPER(col_a)",
    )
    
    added_lineage = mock_session.add.call_args[0][0]
    assert added_lineage.source_column == "col_a"
    assert added_lineage.target_column == "col_b"
    assert lineage_id == 123


def test_log_lineage_raises_on_sqlalchemy_error():
    """Test log_lineage raises DataLineageError on SQLAlchemy error."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    mock_session.flush.side_effect = SQLAlchemyError("DB error")
    tracker._session_factory = Mock(return_value=mock_session)

    with pytest.raises(DataLineageError, match="Failed to log data lineage"):
        tracker.log_lineage(
            process_log_id=1,
            source_schema="bronze",
            source_table="test",
            target_schema="silver",
            target_table="test",
        )

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_log_table_lineage_success():
    """Test log_table_lineage logs multiple source tables."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    lineage_counter = [100]
    
    def mock_flush():
        # Get the most recently added lineage
        if mock_session.add.call_args_list:
            latest_add = mock_session.add.call_args_list[-1]
            added_obj = latest_add[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = lineage_counter[0]
                lineage_counter[0] += 1
    
    mock_session.flush.side_effect = mock_flush
    
    source_tables = [
        ("bronze", "customers"),
        ("bronze", "orders"),
    ]
    target_table = ("silver", "customer_orders")
    
    lineage_ids = tracker.log_table_lineage(
        process_log_id=1,
        source_tables=source_tables,
        target_table=target_table,
        transformation_logic="JOIN customers and orders",
        record_count=5000,
    )
    
    assert len(lineage_ids) == 2
    assert lineage_ids == [100, 101]
    # Each log_lineage call creates a new session, so we expect 2 separate add calls
    assert mock_session.add.call_count == 2


def test_log_column_lineage_success():
    """Test log_column_lineage logs multiple column mappings."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    lineage_counter = [200]
    
    def mock_flush():
        # Get the most recently added lineage
        if mock_session.add.call_args_list:
            latest_add = mock_session.add.call_args_list[-1]
            added_obj = latest_add[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = lineage_counter[0]
                lineage_counter[0] += 1
    
    mock_session.flush.side_effect = mock_flush
    
    column_mappings = [
        {
            'source_schema': 'bronze',
            'source_table': 'raw',
            'source_column': 'first_name',
            'target_schema': 'silver',
            'target_table': 'clean',
            'target_column': 'first_name',
            'transformation': 'TRIM(UPPER(first_name))',
        },
        {
            'source_schema': 'bronze',
            'source_table': 'raw',
            'source_column': 'last_name',
            'target_schema': 'silver',
            'target_table': 'clean',
            'target_column': 'last_name',
            'transformation': 'TRIM(UPPER(last_name))',
        },
    ]
    
    lineage_ids = tracker.log_column_lineage(
        process_log_id=1,
        column_mappings=column_mappings,
        transformation_logic="Name standardization",
    )
    
    assert len(lineage_ids) == 2
    assert lineage_ids == [200, 201]


# ============================================================================
# UNIT TESTS - LineageAnalyzer
# ============================================================================


def test_lineage_analyzer_initialization():
    """Test LineageAnalyzer initializes correctly."""
    analyzer = LineageAnalyzer(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert analyzer.host == "test_host"
    assert analyzer.port == 5555


def test_get_upstream_lineage_returns_lineage():
    """Test get_upstream_lineage returns upstream dependencies."""
    analyzer = LineageAnalyzer()
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results - upstream lineage query columns (based on actual implementation):
    # Columns: source_schema, source_table, source_column, target_schema, target_table, target_column, transformation_logic, record_count, depth, created_timestamp
    mock_row1 = ['bronze', 'raw_customers', None, 'silver', 'customers', None, 'Initial load', 1000, 1, '2024-01-01 12:00:00']
    mock_row2 = ['source_crm', 'customers', None, 'bronze', 'raw_customers', None, 'Extract from CRM', 1200, 2, '2024-01-01 11:00:00']
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    # Setup context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    analyzer._get_engine = Mock(return_value=mock_engine)
    
    result = analyzer.get_upstream_lineage(
        target_schema="silver",
        target_table="customers",
        max_depth=5,
    )
    
    assert 'target_table' in result
    assert result['target_table'] == 'silver.customers'
    assert 'lineage_by_depth' in result
    assert 'total_upstream_tables' in result
    assert result['total_upstream_tables'] == 2


def test_get_downstream_lineage_returns_lineage():
    """Test get_downstream_lineage returns downstream dependencies."""
    analyzer = LineageAnalyzer()
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results - downstream lineage query columns (based on actual implementation):
    # Columns: source_schema, source_table, source_column, target_schema, target_table, target_column, transformation_logic, record_count, depth, created_timestamp
    mock_row1 = ['bronze', 'raw_customers', None, 'silver', 'customers', None, 'Cleansing', 950, 1, '2024-01-01 12:00:00']
    mock_row2 = ['silver', 'customers', None, 'gold', 'customer_summary', None, 'Aggregation', 100, 2, '2024-01-01 13:00:00']
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    analyzer._get_engine = Mock(return_value=mock_engine)
    
    result = analyzer.get_downstream_lineage(
        source_schema="bronze",
        source_table="raw_customers",
        max_depth=5,
    )
    
    assert 'source_table' in result
    assert result['source_table'] == 'bronze.raw_customers'
    assert 'lineage_by_depth' in result
    assert 'total_downstream_tables' in result
    assert result['total_downstream_tables'] == 2


def test_get_medallion_flow_returns_flow():
    """Test get_medallion_flow returns complete medallion architecture flow."""
    analyzer = LineageAnalyzer()
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results - medallion flow query columns (based on actual implementation):
    # Columns: source_schema, source_table, target_schema, target_table, transformation_count, unique_transformations, latest_update, total_records_processed
    mock_row1 = ['bronze', 'raw_sales', 'silver', 'sales', 1, 1, '2024-01-01 12:00:00', 10000]
    mock_row2 = ['silver', 'sales', 'gold', 'sales_summary', 1, 1, '2024-01-01 13:00:00', 1000]
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    analyzer._get_engine = Mock(return_value=mock_engine)
    
    result = analyzer.get_medallion_flow()
    
    assert 'bronze_to_silver' in result
    assert 'silver_to_gold' in result
    assert 'other_flows' in result
    assert len(result['bronze_to_silver']) == 1
    assert len(result['silver_to_gold']) == 1


# ============================================================================
# UNIT TESTS - ImpactAnalyzer
# ============================================================================


def test_impact_analyzer_initialization():
    """Test ImpactAnalyzer initializes correctly."""
    mock_analyzer = Mock(spec=LineageAnalyzer)
    impact = ImpactAnalyzer(mock_analyzer)
    
    assert impact.lineage_analyzer is mock_analyzer


def test_analyze_impact_returns_impact_analysis():
    """Test analyze_impact returns impact analysis."""
    mock_analyzer = Mock(spec=LineageAnalyzer)
    
    # Mock downstream lineage - must match actual return structure
    mock_analyzer.get_downstream_lineage.return_value = {
        'source_table': 'bronze.customers',
        'max_depth_analyzed': 5,
        'lineage_by_depth': {
            1: [
                {
                    'source_schema': 'bronze',
                    'source_table': 'customers',
                    'source_column': None,
                    'target_schema': 'silver',
                    'target_table': 'customers',
                    'target_column': None,
                    'transformation_logic': 'Cleansing',
                    'record_count': 950,
                    'created_timestamp': '2024-01-01 12:00:00',
                },
            ],
            2: [
                {
                    'source_schema': 'silver',
                    'source_table': 'customers',
                    'source_column': None,
                    'target_schema': 'gold',
                    'target_table': 'customer_summary',
                    'target_column': None,
                    'transformation_logic': 'Aggregation',
                    'record_count': 100,
                    'created_timestamp': '2024-01-01 13:00:00',
                },
            ],
        },
        'total_downstream_tables': 2,
    }
    
    impact = ImpactAnalyzer(mock_analyzer)
    result = impact.analyze_impact(
        changed_schema="bronze",
        changed_table="customers",
        change_type="SCHEMA_CHANGE",
    )
    
    assert 'changed_table' in result
    assert result['changed_table'] == 'bronze.customers'
    assert 'change_type' in result
    assert result['change_type'] == 'SCHEMA_CHANGE'
    assert 'impact_severity' in result
    assert 'affected_tables_count' in result
    assert 'recommendations' in result


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_lineage_tracking_lifecycle():
    """Test complete lineage tracking lifecycle."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    lineage_counter = [1]
    
    def mock_flush():
        for call_args in mock_session.add.call_args_list:
            added_obj = call_args[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = lineage_counter[0]
                lineage_counter[0] += 1
    
    mock_session.flush.side_effect = mock_flush
    
    # Log table lineage
    lineage_id = tracker.log_lineage(
        process_log_id=1,
        source_schema="bronze",
        source_table="raw_data",
        target_schema="silver",
        target_table="clean_data",
        transformation_logic="Data cleansing",
        record_count=1000,
    )
    
    assert lineage_id == 1
    assert mock_session.add.called
    assert mock_session.commit.called


def test_lineage_analysis_workflow():
    """Test lineage analysis workflow."""
    analyzer = LineageAnalyzer()
    mock_engine = Mock()
    mock_connection = Mock()
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = []
    mock_connection.execute.return_value = mock_exec_result
    
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    analyzer._get_engine = Mock(return_value=mock_engine)
    
    # Get upstream lineage
    upstream = analyzer.get_upstream_lineage("silver", "customers")
    assert 'lineage_by_depth' in upstream
    assert 'target_table' in upstream
    
    # Get downstream lineage
    downstream = analyzer.get_downstream_lineage("bronze", "raw_customers")
    assert 'lineage_by_depth' in downstream
    assert 'source_table' in downstream


# ============================================================================
# EDGE CASES AND SMOKE TESTS
# ============================================================================


def test_log_lineage_minimal_params():
    """Test log_lineage with minimal required parameters."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    def mock_flush():
        for call_args in mock_session.add.call_args_list:
            added_obj = call_args[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = 999
    
    mock_session.flush.side_effect = mock_flush
    
    lineage_id = tracker.log_lineage(
        process_log_id=1,
        source_schema="bronze",
        source_table="source",
        target_schema="silver",
        target_table="target",
    )
    
    added_lineage = mock_session.add.call_args[0][0]
    assert added_lineage.source_column is None
    assert added_lineage.target_column is None
    assert added_lineage.transformation_logic is None
    assert added_lineage.record_count is None
    assert lineage_id == 999


def test_log_table_lineage_single_source():
    """Test log_table_lineage with single source table."""
    tracker = LineageTracker()
    mock_session = Mock(spec=Session)
    tracker._session_factory = Mock(return_value=mock_session)
    
    def mock_flush():
        for call_args in mock_session.add.call_args_list:
            added_obj = call_args[0][0]
            if isinstance(added_obj, DataLineage):
                added_obj.lineage_id = 50
    
    mock_session.flush.side_effect = mock_flush
    
    source_tables = [("bronze", "single_source")]
    target_table = ("silver", "target")
    
    lineage_ids = tracker.log_table_lineage(
        process_log_id=1,
        source_tables=source_tables,
        target_table=target_table,
    )
    
    assert len(lineage_ids) == 1
    assert lineage_ids[0] == 50


def test_log_column_lineage_empty_mappings():
    """Test log_column_lineage with empty mappings."""
    tracker = LineageTracker()
    
    lineage_ids = tracker.log_column_lineage(
        process_log_id=1,
        column_mappings=[],
    )
    
    assert lineage_ids == []


def test_get_upstream_lineage_with_max_depth():
    """Test get_upstream_lineage respects max_depth parameter."""
    analyzer = LineageAnalyzer()
    mock_engine = Mock()
    mock_connection = Mock()
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = []
    mock_connection.execute.return_value = mock_exec_result
    
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    analyzer._get_engine = Mock(return_value=mock_engine)
    
    result = analyzer.get_upstream_lineage(
        target_schema="gold",
        target_table="summary",
        max_depth=10,
    )
    
    assert 'max_depth_analyzed' in result
    assert result['max_depth_analyzed'] == 10


def test_analyze_impact_with_different_change_types():
    """Test analyze_impact handles different change types."""
    mock_analyzer = Mock(spec=LineageAnalyzer)
    mock_analyzer.get_downstream_lineage.return_value = {
        'source_table': 'bronze.test',
        'max_depth_analyzed': 5,
        'lineage_by_depth': {},
        'total_downstream_tables': 0,
    }
    
    impact = ImpactAnalyzer(mock_analyzer)
    
    # Test different change types
    for change_type in ['SCHEMA_CHANGE', 'DATA_QUALITY_ISSUE', 'DATA_UPDATE']:
        result = impact.analyze_impact(
            changed_schema="bronze",
            changed_table="test",
            change_type=change_type,
        )
        assert result['change_type'] == change_type


def test_smoke_import_lineage_tracker():
    """Smoke test: import and instantiate LineageTracker."""
    from logs.data_lineage import LineageTracker
    tracker = LineageTracker()
    assert tracker is not None
    assert hasattr(tracker, 'log_lineage')


def test_smoke_import_lineage_analyzer():
    """Smoke test: import and instantiate LineageAnalyzer."""
    from logs.data_lineage import LineageAnalyzer
    analyzer = LineageAnalyzer()
    assert analyzer is not None
    assert hasattr(analyzer, 'get_upstream_lineage')


def test_smoke_import_impact_analyzer():
    """Smoke test: import and instantiate ImpactAnalyzer."""
    from logs.data_lineage import ImpactAnalyzer, LineageAnalyzer
    analyzer = LineageAnalyzer()
    impact = ImpactAnalyzer(analyzer)
    assert impact is not None
    assert hasattr(impact, 'analyze_impact')


def test_smoke_data_lineage_error_exception():
    """Smoke test: DataLineageError can be raised and caught."""
    from logs.data_lineage import DataLineageError
    
    with pytest.raises(DataLineageError):
        raise DataLineageError("Test error")


def test_smoke_engine_attribute():
    """Smoke test: LineageTracker and LineageAnalyzer have _engine attribute."""
    tracker = LineageTracker()
    analyzer = LineageAnalyzer()
    
    assert hasattr(tracker, '_engine')
    assert hasattr(analyzer, '_engine')
    assert tracker._engine is None
    assert analyzer._engine is None
