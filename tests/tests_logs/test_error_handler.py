"""
Comprehensive test suite for logs.error_handler module.

Tests cover:
- ErrorLogger: log_error, log_exception, mark_error_resolved, get_unresolved_errors
- ErrorRecovery: retry_with_backoff, circuit_breaker
- ErrorAnalyzer: analyze_error_patterns, generate_error_report
- Session management and context manager behavior
- Edge cases and error conditions
"""

import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from logs.error_handler import (
    ErrorAnalyzer,
    ErrorHandlerError,
    ErrorLogger,
    ErrorRecovery,
)
from models.logs_models import ErrorLog, ProcessLog

# ============================================================================
# UNIT TESTS - ErrorLogger
# ============================================================================


def test_error_logger_initialization():
    """Test ErrorLogger initializes with correct database parameters."""
    logger = ErrorLogger(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert logger.host == "test_host"
    assert logger.port == 5555
    assert logger.database == "test_db"
    assert logger.user == "test_user"
    assert logger.password == "test_pass"
    assert logger._engine is None
    assert logger._session_factory is None


def test_get_session_context_manager(mock_error_logger):
    """Test _get_session yields session and commits on success."""
    mock_session = Mock(spec=Session)
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    with mock_error_logger._get_session() as session:
        assert session is mock_session

    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()
    mock_session.rollback.assert_not_called()


def test_get_session_rollback_on_exception(mock_error_logger):
    """Test _get_session rolls back on exception and re-raises."""
    mock_session = Mock(spec=Session)
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    with pytest.raises(RuntimeError, match="Test error"):
        with mock_error_logger._get_session():
            raise RuntimeError("Test error")

    mock_session.rollback.assert_called_once()
    mock_session.commit.assert_not_called()
    mock_session.close.assert_called_once()


def test_log_error_success(mock_error_logger, mock_session):
    """Test log_error creates error log entry successfully."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    error_id = mock_error_logger.log_error(
        process_log_id=1,
        error_message="Test error message",
        error_level="ERROR",
        error_code="TEST_ERROR",
        error_detail="Detailed error info",
        table_name="test_table",
        column_name="test_column",
        row_context="row data",
        recovery_suggestion="Fix it",
    )

    # Verify session operations
    assert mock_session.add.called
    mock_session.flush.assert_called_once()
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()

    # Verify error log object
    added_log = mock_session.add.call_args[0][0]
    assert isinstance(added_log, ErrorLog)
    assert added_log.process_log_id == 1
    assert added_log.error_message == "Test error message"
    assert added_log.error_level == "ERROR"
    assert added_log.error_code == "TEST_ERROR"
    assert added_log.error_detail == "Detailed error info"
    assert added_log.table_name == "test_table"
    assert added_log.column_name == "test_column"
    assert added_log.row_context == "row data"
    assert added_log.recovery_suggestion == "Fix it"
    assert error_id == 123  # From mock


def test_log_error_with_exception(mock_error_logger, mock_session):
    """Test log_error auto-extracts details from exception."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    test_exception = ValueError("Test exception")
    
    with patch('traceback.format_exc', return_value="Traceback details"):
        error_id = mock_error_logger.log_error(
            process_log_id=1,
            error_message="Error occurred",
            exception=test_exception,
        )

    added_log = mock_session.add.call_args[0][0]
    assert added_log.error_code == "ValueError"
    assert added_log.error_detail == "Traceback details"
    assert error_id == 123


def test_log_error_minimal_params(mock_error_logger, mock_session):
    """Test log_error with only required parameters."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    error_id = mock_error_logger.log_error(
        process_log_id=1,
        error_message="Minimal error",
    )

    added_log = mock_session.add.call_args[0][0]
    assert added_log.process_log_id == 1
    assert added_log.error_message == "Minimal error"
    assert added_log.error_level == "ERROR"  # Default
    assert error_id == 123


def test_log_error_raises_on_sqlalchemy_error(mock_error_logger):
    """Test log_error raises ErrorHandlerError on SQLAlchemy error."""
    mock_session = Mock(spec=Session)
    mock_session.flush.side_effect = SQLAlchemyError("DB error")
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    with pytest.raises(ErrorHandlerError, match="Failed to log error"):
        mock_error_logger.log_error(
            process_log_id=1,
            error_message="Test message",
        )

    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


def test_log_exception_success(mock_error_logger, mock_session):
    """Test log_exception captures exception details."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    try:
        raise ValueError("Test exception")
    except ValueError as e:
        with patch('traceback.format_exc', return_value="Full traceback"):
            error_id = mock_error_logger.log_exception(
                process_log_id=1,
                exception=e,
                context={"table_name": "test_table", "step": "processing"},
                recovery_suggestion="Retry the operation",
            )

    added_log = mock_session.add.call_args[0][0]
    assert added_log.process_log_id == 1
    assert added_log.error_message == "Test exception"
    assert added_log.error_code == "ValueError"
    assert added_log.error_detail == "Full traceback"
    assert added_log.table_name == "test_table"
    assert '"table_name": "test_table"' in added_log.row_context
    assert added_log.recovery_suggestion == "Retry the operation"
    assert error_id == 123


def test_mark_error_resolved_success(mock_error_logger, mock_session):
    """Test mark_error_resolved updates error log."""
    mock_error = Mock(spec=ErrorLog)
    mock_error.error_id = 42
    mock_error.recovery_suggestion = None
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_error
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    mock_error_logger.mark_error_resolved(
        error_id=42,
        resolved_by="admin",
        resolution_notes="Fixed by manual intervention",
    )

    assert mock_error.is_resolved is True
    assert mock_error.resolved_by == "admin"
    assert isinstance(mock_error.resolved_timestamp, datetime)
    assert mock_error.recovery_suggestion == "Fixed by manual intervention"
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def test_mark_error_resolved_appends_to_existing_suggestion(mock_error_logger, mock_session):
    """Test mark_error_resolved appends resolution notes to existing suggestion."""
    mock_error = Mock(spec=ErrorLog)
    mock_error.error_id = 42
    mock_error.recovery_suggestion = "Existing suggestion"
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_error
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    mock_error_logger.mark_error_resolved(
        error_id=42,
        resolved_by="admin",
        resolution_notes="Actually fixed it",
    )

    assert "Existing suggestion" in mock_error.recovery_suggestion
    assert "Actually fixed it" in mock_error.recovery_suggestion


def test_mark_error_resolved_not_found_raises_error(mock_error_logger, mock_session):
    """Test mark_error_resolved raises when error not found."""
    mock_session.query.return_value.filter_by.return_value.first.return_value = None
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    with pytest.raises(ErrorHandlerError, match="Error log with ID 999 not found"):
        mock_error_logger.mark_error_resolved(error_id=999, resolved_by="admin")

    mock_session.close.assert_called_once()


def test_get_unresolved_errors_returns_errors(mock_error_logger, mock_session):
    """Test get_unresolved_errors returns list of unresolved errors."""
    mock_error1 = Mock(spec=ErrorLog)
    mock_error1.error_id = 1
    mock_error1.process_log_id = 10
    mock_error1.error_timestamp = datetime.now()
    mock_error1.error_level = "ERROR"
    mock_error1.error_code = "ERR1"
    mock_error1.error_message = "Error 1"
    mock_error1.table_name = "table1"
    mock_error1.column_name = "col1"
    mock_error1.recovery_suggestion = "Fix 1"

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value.order_by.return_value.all.return_value = [mock_error1]
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    errors = mock_error_logger.get_unresolved_errors()

    assert len(errors) == 1
    assert errors[0]['error_id'] == 1
    assert errors[0]['process_log_id'] == 10
    assert errors[0]['error_level'] == "ERROR"
    mock_session.close.assert_called_once()


def test_get_unresolved_errors_with_filters(mock_error_logger, mock_session):
    """Test get_unresolved_errors with process_name and error_level filters."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)
    
    mock_query = mock_session.query.return_value
    mock_filtered = mock_query.filter.return_value
    mock_filtered.filter.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = []

    errors = mock_error_logger.get_unresolved_errors(
        process_name="test_process",
        error_level="CRITICAL",
        days=14,
    )

    assert errors == []
    # Verify filters were applied
    assert mock_filtered.filter.called
    mock_session.close.assert_called_once()


def test_error_logger_has_engine_attribute(mock_error_logger):
    """Test ErrorLogger has _engine attribute."""
    assert hasattr(mock_error_logger, '_engine')
    assert mock_error_logger._engine is None


# ============================================================================
# UNIT TESTS - ErrorRecovery
# ============================================================================


def test_error_recovery_initialization():
    """Test ErrorRecovery initializes with correct parameters."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(
        error_logger=mock_logger,
        max_retries=5,
        base_delay=2.0,
        backoff_multiplier=3.0,
    )
    assert recovery.error_logger is mock_logger
    assert recovery.max_retries == 5
    assert recovery.base_delay == 2.0
    assert recovery.backoff_multiplier == 3.0


def test_retry_with_backoff_success_first_attempt():
    """Test retry_with_backoff succeeds on first attempt."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=3)
    func = Mock(return_value="success")

    result = recovery.retry_with_backoff(func, args=("arg1",), kwargs={"key": "value"})

    assert result == "success"
    func.assert_called_once_with("arg1", key="value")
    mock_logger.log_error.assert_not_called()


def test_retry_with_backoff_success_after_retries():
    """Test retry_with_backoff succeeds after retries."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=3, base_delay=0.01)
    func = Mock(side_effect=[ValueError("fail1"), ValueError("fail2"), "success"])

    with patch("time.sleep"):
        result = recovery.retry_with_backoff(
            func,
            process_log_id=1,
            retryable_exceptions=(ValueError,),
        )

    assert result == "success"
    assert func.call_count == 3
    # Should have logged 2 warning-level retry attempts
    assert mock_logger.log_error.call_count == 2


def test_retry_with_backoff_all_attempts_fail():
    """Test retry_with_backoff raises after all attempts fail."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=2, base_delay=0.01)
    func = Mock(side_effect=ValueError("persistent error"))

    with patch("time.sleep"):
        with pytest.raises(ValueError, match="persistent error"):
            recovery.retry_with_backoff(
                func,
                process_log_id=1,
                retryable_exceptions=(ValueError,),
            )

    assert func.call_count == 3  # Initial + 2 retries
    # Should have logged 2 warnings + 1 final error
    assert mock_logger.log_error.call_count == 3


def test_retry_with_backoff_exponential_delay():
    """Test retry_with_backoff uses exponential backoff."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=3, base_delay=1.0, backoff_multiplier=2.0)
    func = Mock(side_effect=[ValueError(), ValueError(), ValueError(), "success"])

    with patch("time.sleep") as mock_sleep:
        recovery.retry_with_backoff(func, retryable_exceptions=(ValueError,))

    # Verify exponential backoff: 1.0, 2.0, 4.0
    assert mock_sleep.call_count == 3
    delays = [call[0][0] for call in mock_sleep.call_args_list]
    assert delays == [1.0, 2.0, 4.0]


def test_retry_with_backoff_non_retryable_exception():
    """Test retry_with_backoff doesn't retry non-retryable exceptions."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=3)
    func = Mock(side_effect=RuntimeError("not retryable"))

    with pytest.raises(RuntimeError, match="not retryable"):
        recovery.retry_with_backoff(
            func,
            retryable_exceptions=(ValueError,),  # Only retry ValueError
        )

    assert func.call_count == 1  # Should not retry
    mock_logger.log_error.assert_not_called()


def test_circuit_breaker_success():
    """Test circuit_breaker allows successful calls."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger)
    func = Mock(return_value="success", __name__="test_func")

    result = recovery.circuit_breaker(func, process_log_id=1)

    assert result == "success"
    func.assert_called_once()
    mock_logger.log_error.assert_not_called()


def test_circuit_breaker_logs_failures():
    """Test circuit_breaker logs failures."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger)
    func = Mock(side_effect=RuntimeError("circuit error"), __name__="test_func")

    with pytest.raises(RuntimeError, match="circuit error"):
        recovery.circuit_breaker(func, process_log_id=1, failure_threshold=3)

    func.assert_called_once()
    mock_logger.log_error.assert_called_once()
    # Verify it logged a WARNING level circuit breaker error
    call_args = mock_logger.log_error.call_args
    assert call_args[1]['error_level'] == 'WARNING'
    assert 'CIRCUIT_BREAKER' in call_args[1]['error_code']


# ============================================================================
# UNIT TESTS - ErrorAnalyzer
# ============================================================================


def test_error_analyzer_initialization():
    """Test ErrorAnalyzer initializes with correct parameters."""
    analyzer = ErrorAnalyzer(
        host="test_host",
        port=5555,
        database="test_db",
        user="test_user",
        password="test_pass",
    )
    assert analyzer.host == "test_host"
    assert analyzer.port == 5555


def test_analyze_error_patterns_returns_summary(mock_error_analyzer):
    """Test analyze_error_patterns returns error pattern summary."""
    mock_engine = Mock()
    mock_connection = Mock()
    
    # Create mock results that simulate fetchall()
    mock_result_freq = [
        Mock(_mapping={'error_code': 'ERR1', 'error_level': 'ERROR', 'error_count': 10, 'resolved_count': 5, 'avg_resolution_hours': 2.5}),
        Mock(_mapping={'error_code': 'ERR2', 'error_level': 'WARNING', 'error_count': 3, 'resolved_count': 3, 'avg_resolution_hours': 1.0}),
    ]
    # Make rows subscriptable for total_errors calculation
    mock_result_freq[0].__getitem__ = lambda s, i: [None, None, 10, 5, 2.5][i]
    mock_result_freq[1].__getitem__ = lambda s, i: [None, None, 3, 3, 1.0][i]
    
    # Mock execute returns objects with fetchall() method
    mock_exec_result1 = Mock()
    mock_exec_result1.fetchall.return_value = mock_result_freq
    mock_exec_result2 = Mock()
    mock_exec_result2.fetchall.return_value = []
    mock_exec_result3 = Mock()
    mock_exec_result3.fetchall.return_value = []
    
    mock_connection.execute.side_effect = [
        mock_exec_result1,  # error_frequency
        mock_exec_result2,  # error_trends
        mock_exec_result3,  # problematic_tables
    ]
    
    # Setup context manager for engine.connect()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_connection
    mock_context.__exit__.return_value = None
    mock_engine.connect.return_value = mock_context
    mock_error_analyzer._get_engine = Mock(return_value=mock_engine)

    patterns = mock_error_analyzer.analyze_error_patterns(days=30)

    assert patterns['period_days'] == 30
    assert patterns['summary']['total_errors'] == 13
    assert patterns['summary']['unique_error_types'] == 2
    assert patterns['summary']['resolution_rate'] > 0


def test_generate_error_report_creates_report(mock_error_analyzer):
    """Test generate_error_report creates formatted report."""
    # Mock analyze_error_patterns to return test data
    mock_analysis = {
        'summary': {
            'total_errors': 50,
            'unique_error_types': 5,
            'resolution_rate': 75.5,
        },
        'error_frequency': [
            {'error_code': 'ERR1', 'error_level': 'ERROR', 'error_count': 20},
            {'error_code': 'ERR2', 'error_level': 'WARNING', 'error_count': 15},
        ],
        'problematic_tables': [
            {'table_name': 'table1', 'error_count': 30, 'unique_error_types': 3},
        ],
    }
    
    with patch.object(mock_error_analyzer, 'analyze_error_patterns', return_value=mock_analysis):
        report = mock_error_analyzer.generate_error_report(days=7)

    assert "ERROR ANALYSIS REPORT" in report
    assert "Total Errors: 50" in report
    assert "Unique Error Types: 5" in report
    assert "Resolution Rate: 75.5%" in report
    assert "ERR1" in report
    assert "table1" in report


def test_generate_error_report_handles_errors(mock_error_analyzer):
    """Test generate_error_report handles analysis errors gracefully."""
    with patch.object(mock_error_analyzer, 'analyze_error_patterns', side_effect=Exception("Analysis failed")):
        report = mock_error_analyzer.generate_error_report(days=7)

    assert "Error generating report" in report
    assert "Analysis failed" in report


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_error_logging_lifecycle(mock_error_logger, mock_session):
    """Test complete error logging and resolution lifecycle."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    # Log error
    error_id = mock_error_logger.log_error(
        process_log_id=1,
        error_message="Test error",
        error_level="ERROR",
    )
    assert error_id == 123

    # Mark as resolved
    mock_error = Mock(spec=ErrorLog)
    mock_error.error_id = 123
    mock_error.recovery_suggestion = None
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_error

    mock_error_logger.mark_error_resolved(error_id=123, resolved_by="admin")
    assert mock_error.is_resolved is True


def test_retry_recovery_with_logging():
    """Test retry mechanism logs attempts."""
    mock_logger = Mock(spec=ErrorLogger)
    recovery = ErrorRecovery(error_logger=mock_logger, max_retries=2, base_delay=0.01)
    
    attempt_count = {"count": 0}

    def flaky_function():
        attempt_count["count"] += 1
        if attempt_count["count"] < 3:
            raise ValueError(f"Attempt {attempt_count['count']} failed")
        return "success"

    with patch("time.sleep"):
        result = recovery.retry_with_backoff(
            flaky_function,
            process_log_id=1,
            retryable_exceptions=(ValueError,),
        )

    assert result == "success"
    assert attempt_count["count"] == 3
    # Should have logged 2 retry attempts (warnings)
    assert mock_logger.log_error.call_count == 2


# ============================================================================
# EDGE CASES AND SMOKE TESTS
# ============================================================================


def test_log_error_with_very_long_message(mock_error_logger, mock_session):
    """Test log_error handles very long error messages."""
    mock_error_logger._session_factory = Mock(return_value=mock_session)
    long_message = "x" * 10000

    error_id = mock_error_logger.log_error(
        process_log_id=1,
        error_message=long_message,
    )

    added_log = mock_session.add.call_args[0][0]
    assert len(added_log.error_message) == 10000
    assert error_id == 123


def test_get_unresolved_errors_empty_result(mock_error_logger, mock_session):
    """Test get_unresolved_errors returns empty list when no errors."""
    mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
    mock_error_logger._session_factory = Mock(return_value=mock_session)

    errors = mock_error_logger.get_unresolved_errors()

    assert errors == []
    mock_session.close.assert_called_once()


def test_smoke_import_error_logger():
    """Smoke test: ErrorLogger can be imported."""
    from logs.error_handler import ErrorLogger

    assert ErrorLogger is not None


def test_smoke_import_error_recovery():
    """Smoke test: ErrorRecovery can be imported."""
    from logs.error_handler import ErrorRecovery

    assert ErrorRecovery is not None


def test_smoke_import_error_analyzer():
    """Smoke test: ErrorAnalyzer can be imported."""
    from logs.error_handler import ErrorAnalyzer

    assert ErrorAnalyzer is not None


def test_smoke_error_handler_error_exception():
    """Smoke test: ErrorHandlerError can be raised."""
    with pytest.raises(ErrorHandlerError):
        raise ErrorHandlerError("Test error")


def test_smoke_engine_attribute(mock_error_logger):
    """Smoke test: ErrorLogger has _engine attribute."""
    assert hasattr(mock_error_logger, '_engine')
    mock_error_logger._engine = None  # Should not raise
    assert mock_error_logger._engine is None
