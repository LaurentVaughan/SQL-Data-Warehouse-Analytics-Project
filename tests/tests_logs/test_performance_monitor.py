"""
Comprehensive test suite for logs.performance_monitor module.

Tests cover:
- PerformanceMonitor: record_metric, monitor_process context manager
- ProcessMonitor: start, end, record_metric, system metrics collection
- MetricsCollector: get_performance_summary, get_throughput_analysis
- ThroughputAnalyzer: identify_bottlenecks, analyze_performance
- Session management and context manager behavior
- Edge cases and error conditions
"""

from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from logs.performance_monitor import (
    MetricsCollector,
    PerformanceMonitor,
    PerformanceMonitorError,
    ProcessMonitor,
    ThroughputAnalyzer,
)
from models.logs_models import PerformanceMetrics

# ============================================================================
# UNIT TESTS - PerformanceMonitor
# ============================================================================


def test_performance_monitor_initialization():
    """Test PerformanceMonitor initializes with correct database parameters."""
    monitor = PerformanceMonitor(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert monitor.host == "test_host"
    assert monitor.port == 5555
    assert monitor.database == "test_db"
    assert monitor.user == "test_user"
    assert monitor.password == "test_pass"
    assert monitor._engine is None
    assert monitor._session_factory is None


def test_get_session_context_manager(mock_performance_monitor):
    """Test _get_session yields session and commits on success."""
    mock_session = Mock(spec=Session)
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    with mock_performance_monitor._get_session() as session:
        assert session is mock_session

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()
    mock_session.rollback.assert_not_called()


def test_get_session_rollback_on_exception(mock_performance_monitor):
    """Test _get_session rolls back on exception and re-raises."""
    mock_session = Mock(spec=Session)
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    with pytest.raises(RuntimeError, match="Test error"):
        with mock_performance_monitor._get_session():
            raise RuntimeError("Test error")

    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()
    mock_session.close.assert_called_once()


def test_record_metric_success(mock_performance_monitor, mock_session):
    """Test record_metric creates performance metric entry successfully."""
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    metric_id = mock_performance_monitor.record_metric(
        process_log_id=1,
        metric_name="cpu_usage",
        metric_value=75.5,
        metric_unit="percent",
        additional_context="Peak usage during processing",
    )

    # Verify session operations
    assert mock_session.add.called
    mock_session.flush.assert_called_once()
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()

    # Verify performance metric object
    added_metric = mock_session.add.call_args[0][0]
    assert isinstance(added_metric, PerformanceMetrics)
    assert added_metric.process_log_id == 1
    assert added_metric.metric_name == "cpu_usage"
    assert added_metric.metric_value == 75.5
    assert added_metric.metric_unit == "percent"
    assert added_metric.additional_context == "Peak usage during processing"
    assert metric_id == 456  # From mock


def test_record_metric_minimal_params(mock_performance_monitor, mock_session):
    """Test record_metric with only required parameters."""
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    metric_id = mock_performance_monitor.record_metric(
        process_log_id=1,
        metric_name="rows_processed",
        metric_value=1000.0,
    )

    added_metric = mock_session.add.call_args[0][0]
    assert added_metric.process_log_id == 1
    assert added_metric.metric_name == "rows_processed"
    assert added_metric.metric_value == 1000.0
    assert added_metric.metric_unit is None
    assert added_metric.additional_context is None
    assert metric_id == 456


def test_record_metric_raises_on_sqlalchemy_error(mock_performance_monitor):
    """Test record_metric raises PerformanceMonitorError on SQLAlchemy error."""
    mock_session = Mock(spec=Session)
    mock_session.flush.side_effect = SQLAlchemyError("DB error")
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    with pytest.raises(PerformanceMonitorError, match="Failed to record metric"):
        mock_performance_monitor.record_metric(
            process_log_id=1,
            metric_name="test_metric",
            metric_value=100.0,
        )

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_monitor_process_context_manager(mock_performance_monitor):
    """Test monitor_process yields ProcessMonitor and calls start/end."""
    with patch.object(ProcessMonitor, 'start') as mock_start, \
         patch.object(ProcessMonitor, 'end') as mock_end:
        
        with mock_performance_monitor.monitor_process(process_log_id=1) as monitor:
            assert isinstance(monitor, ProcessMonitor)
            assert monitor.performance_monitor is mock_performance_monitor
            assert monitor.process_log_id == 1
        
        mock_start.assert_called_once()
        mock_end.assert_called_once()


def test_monitor_process_ensures_end_called_on_exception(mock_performance_monitor):
    """Test monitor_process calls end() even when exception occurs."""
    with patch.object(ProcessMonitor, 'start') as mock_start, \
         patch.object(ProcessMonitor, 'end') as mock_end:
        
        with pytest.raises(ValueError, match="Test error"):
            with mock_performance_monitor.monitor_process(process_log_id=1):
                raise ValueError("Test error")
        
        mock_start.assert_called_once()
        mock_end.assert_called_once()  # Still called despite exception


# ============================================================================
# UNIT TESTS - ProcessMonitor
# ============================================================================


def test_process_monitor_initialization():
    """Test ProcessMonitor initializes correctly."""
    mock_perf_monitor = Mock(spec=PerformanceMonitor)
    monitor = ProcessMonitor(mock_perf_monitor, process_log_id=42)
    
    assert monitor.performance_monitor is mock_perf_monitor
    assert monitor.process_log_id == 42
    assert monitor.start_time is None
    assert monitor.start_cpu_times is None
    assert monitor.start_memory is None


@patch('psutil.cpu_percent')
@patch('psutil.virtual_memory')
@patch('psutil.Process')
def test_process_monitor_start(mock_process, mock_vmem, mock_cpu):
    """Test ProcessMonitor.start() captures system metrics."""
    mock_perf_monitor = Mock(spec=PerformanceMonitor)
    monitor = ProcessMonitor(mock_perf_monitor, process_log_id=1)
    
    # Mock psutil returns
    mock_cpu_times = Mock(user=10.0, system=5.0)
    mock_process.return_value.cpu_times.return_value = mock_cpu_times
    mock_process.return_value.memory_info.return_value.rss = 1024 * 1024 * 100  # 100 MB
    
    monitor.start()
    
    assert monitor.start_time is not None
    assert monitor.start_cpu_times is not None
    assert monitor.start_memory is not None


@patch('psutil.cpu_percent')
@patch('psutil.virtual_memory')
@patch('psutil.Process')
@patch('time.time')
def test_process_monitor_end_records_metrics(mock_time, mock_process, mock_vmem, mock_cpu):
    """Test ProcessMonitor.end() records performance metrics."""
    mock_perf_monitor = Mock(spec=PerformanceMonitor)
    monitor = ProcessMonitor(mock_perf_monitor, process_log_id=1)
    
    # Setup start state using time.time() float values
    start_time = 1000.0
    end_time = 1010.0  # 10 seconds later
    monitor.start_time = start_time
    monitor.start_cpu_times = Mock(user=10.0, system=5.0)
    monitor.start_memory = Mock(rss=1024 * 1024 * 100)  # 100 MB
    
    # Mock current state
    mock_time.return_value = end_time
    mock_cpu_times = Mock(user=15.0, system=8.0)
    mock_process.return_value.cpu_times.return_value = mock_cpu_times
    mock_process.return_value.memory_info.return_value.rss = 1024 * 1024 * 150  # 150 MB
    mock_vmem.return_value.percent = 60.0
    mock_cpu.return_value = 45.0
    
    monitor.end()
    
    # Verify metrics were recorded
    assert mock_perf_monitor.record_metric.called
    # Should have recorded: execution_time, cpu_time, memory_delta, peak_memory, cpu_percent
    assert mock_perf_monitor.record_metric.call_count >= 3


def test_process_monitor_record_metric_delegates_to_performance_monitor():
    """Test ProcessMonitor.record_metric delegates to PerformanceMonitor."""
    mock_perf_monitor = Mock(spec=PerformanceMonitor)
    monitor = ProcessMonitor(mock_perf_monitor, process_log_id=1)
    
    monitor.record_metric("test_metric", 123.45, "units")
    
    mock_perf_monitor.record_metric.assert_called_once_with(
        process_log_id=1,
        metric_name="test_metric",
        metric_value=123.45,
        metric_unit="units",
    )


# ============================================================================
# UNIT TESTS - MetricsCollector
# ============================================================================


def test_metrics_collector_initialization():
    """Test MetricsCollector initializes with correct parameters."""
    collector = MetricsCollector(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert collector.host == "test_host"
    assert collector.port == 5555


def test_get_performance_summary_returns_summary():
    """Test get_performance_summary returns performance metrics summary."""
    collector = MetricsCollector()
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results - returns rows that match the actual SQL query
    # Columns: process_name, metric_name, measurement_count, avg_value, min_value, max_value, std_dev, metric_unit
    mock_row1 = ['bronze_ingestion', 'cpu_percent', 10, 45.5, 20.0, 80.0, 15.2, 'percent']
    mock_row2 = ['bronze_ingestion', 'memory_mb', 10, 512.0, 400.0, 650.0, 50.0, 'MB']
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    # Setup context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    collector._get_engine = Mock(return_value=mock_engine)
    
    summary = collector.get_performance_summary(days=7)
    
    # Verify structure: dict organized by process_name -> metric_name -> stats
    assert 'bronze_ingestion' in summary
    assert 'cpu_percent' in summary['bronze_ingestion']
    assert 'memory_mb' in summary['bronze_ingestion']
    assert summary['bronze_ingestion']['cpu_percent']['avg_value'] == 45.5
    assert summary['bronze_ingestion']['memory_mb']['measurement_count'] == 10


def test_get_throughput_analysis_returns_analysis():
    """Test get_throughput_analysis returns throughput metrics."""
    collector = MetricsCollector()
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results  - matches actual query columns
    # Columns: process_date, executions, avg_rows_processed, total_rows_processed, avg_execution_time, rows_per_second
    mock_row1 = ['2024-01-01', 5, 20000.0, 100000, 30.0, 666.67]
    mock_row2 = ['2024-01-02', 4, 18000.0, 72000, 28.0, 642.86]
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    # Setup context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    collector._get_engine = Mock(return_value=mock_engine)
    
    analysis = collector.get_throughput_analysis(process_name="bronze_ingestion", days=30)
    
    assert 'process_name' in analysis
    assert analysis['process_name'] == 'bronze_ingestion'
    assert 'analysis_period_days' in analysis
    assert analysis['analysis_period_days'] == 30
    assert 'daily_data' in analysis
    assert len(analysis['daily_data']) == 2
    assert 'summary' in analysis
    assert 'total_executions' in analysis['summary']


# ============================================================================
# UNIT TESTS - ThroughputAnalyzer
# ============================================================================


def test_throughput_analyzer_initialization():
    """Test ThroughputAnalyzer initializes correctly."""
    mock_collector = Mock(spec=MetricsCollector)
    analyzer = ThroughputAnalyzer(mock_collector)
    
    assert analyzer.metrics_collector is mock_collector


def test_identify_bottlenecks_returns_bottleneck_analysis():
    """Test identify_bottlenecks identifies performance issues."""
    mock_collector = Mock(spec=MetricsCollector)
    
    # Mock the engine and connection
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Mock SQL results - matches actual query columns
    # Columns: log_id, start_time, execution_time, rows_processed, throughput_rows_per_second, performance_category
    mock_row1 = [101, '2024-01-01 12:00:00', 120.5, 50000, 415.0, 'SLOW_EXECUTION']
    mock_row2 = [102, '2024-01-01 14:00:00', 95.0, 30000, 315.8, 'LOW_THROUGHPUT']
    
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = [mock_row1, mock_row2]
    mock_connection.execute.return_value = mock_exec_result
    
    # Setup context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    mock_collector._get_engine.return_value = mock_engine
    
    analyzer = ThroughputAnalyzer(mock_collector)
    bottlenecks = analyzer.identify_bottlenecks(process_name="bronze_ingestion", threshold_percentile=95.0)
    
    assert 'process_name' in bottlenecks
    assert bottlenecks['process_name'] == 'bronze_ingestion'
    assert 'threshold_percentile' in bottlenecks
    assert bottlenecks['threshold_percentile'] == 95.0
    assert 'bottlenecks_found' in bottlenecks
    assert bottlenecks['bottlenecks_found'] == 2
    assert 'bottlenecks' in bottlenecks
    assert len(bottlenecks['bottlenecks']) == 2


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_performance_monitoring_lifecycle():
    """Test complete performance monitoring lifecycle."""
    from logs.performance_monitor import PerformanceMonitor, ProcessMonitor

    # Create a fully mocked PerformanceMonitor
    mock_monitor = Mock(spec=PerformanceMonitor)
    mock_process_monitor = Mock(spec=ProcessMonitor)
    mock_process_monitor.performance_monitor = mock_monitor
    mock_process_monitor.process_log_id = 1
    
    # Mock the context manager to yield the process monitor
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_process_monitor
    mock_context.__exit__.return_value = None
    mock_monitor.monitor_process.return_value = mock_context
    
    # Use monitor_process context manager
    with mock_monitor.monitor_process(process_log_id=1) as monitor:
        # Record custom metric during processing
        monitor.record_metric("rows_processed", 1000.0, "rows")
    
    # Verify context manager was called
    mock_monitor.monitor_process.assert_called_once_with(process_log_id=1)
    # Verify metric was recorded
    mock_process_monitor.record_metric.assert_called_with(
        "rows_processed", 1000.0, "rows"
    )


def test_metrics_collection_and_analysis():
    """Test metrics collection and analysis workflow."""
    mock_collector = Mock(spec=MetricsCollector)
    
    # Mock the engine and connection for identify_bottlenecks
    mock_engine = Mock()
    mock_connection = Mock()
    mock_exec_result = Mock()
    mock_exec_result.fetchall.return_value = []  # No bottlenecks found
    mock_connection.execute.return_value = mock_exec_result
    
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    mock_collector._get_engine.return_value = mock_engine
    
    analyzer = ThroughputAnalyzer(mock_collector)
    bottlenecks = analyzer.identify_bottlenecks(process_name="test_process", threshold_percentile=95.0)
    
    assert 'bottlenecks' in bottlenecks
    assert 'process_name' in bottlenecks
    assert bottlenecks['process_name'] == 'test_process'
    assert bottlenecks['bottlenecks_found'] == 0


# ============================================================================
# EDGE CASES AND SMOKE TESTS
# ============================================================================


def test_record_metric_with_very_large_value(mock_performance_monitor, mock_session):
    """Test record_metric handles very large metric values."""
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)
    large_value = 999999999.999

    metric_id = mock_performance_monitor.record_metric(
        process_log_id=1,
        metric_name="large_value",
        metric_value=large_value,
    )

    added_metric = mock_session.add.call_args[0][0]
    assert added_metric.metric_value == large_value
    assert metric_id == 456


def test_record_metric_with_zero_value(mock_performance_monitor, mock_session):
    """Test record_metric handles zero values."""
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    metric_id = mock_performance_monitor.record_metric(
        process_log_id=1,
        metric_name="zero_metric",
        metric_value=0.0,
    )

    added_metric = mock_session.add.call_args[0][0]
    assert added_metric.metric_value == 0.0
    assert metric_id == 456


def test_record_metric_with_negative_value(mock_performance_monitor, mock_session):
    """Test record_metric handles negative values."""
    mock_performance_monitor._session_factory = Mock(return_value=mock_session)

    metric_id = mock_performance_monitor.record_metric(
        process_log_id=1,
        metric_name="delta_metric",
        metric_value=-50.5,
    )

    added_metric = mock_session.add.call_args[0][0]
    assert added_metric.metric_value == -50.5
    assert metric_id == 456


def test_smoke_import_performance_monitor():
    """Smoke test: PerformanceMonitor can be imported."""
    from logs.performance_monitor import PerformanceMonitor

    assert PerformanceMonitor is not None


def test_smoke_import_process_monitor():
    """Smoke test: ProcessMonitor can be imported."""
    from logs.performance_monitor import ProcessMonitor

    assert ProcessMonitor is not None


def test_smoke_import_metrics_collector():
    """Smoke test: MetricsCollector can be imported."""
    from logs.performance_monitor import MetricsCollector

    assert MetricsCollector is not None


def test_smoke_import_throughput_analyzer():
    """Smoke test: ThroughputAnalyzer can be imported."""
    from logs.performance_monitor import ThroughputAnalyzer

    assert ThroughputAnalyzer is not None


def test_smoke_performance_monitor_error_exception():
    """Smoke test: PerformanceMonitorError can be raised."""
    with pytest.raises(PerformanceMonitorError):
        raise PerformanceMonitorError("Test error")


def test_smoke_engine_attribute(mock_performance_monitor):
    """Smoke test: PerformanceMonitor has _engine attribute."""
    assert hasattr(mock_performance_monitor, '_engine')
    mock_performance_monitor._engine = None  # Should not raise
    assert mock_performance_monitor._engine is None
