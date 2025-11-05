"""
==============================================
Error handling and recovery for ETL processes.
==============================================

Provides centralized error logging, recovery mechanisms, and error pattern
analysis for the data warehouse. Integrates with the logging infrastructure
for comprehensive error tracking and automated recovery.

Error handling is critical for:
    - Production reliability and uptime
    - Debugging and root cause analysis
    - Automated recovery and retry logic
    - Error pattern detection and prevention

Classes:
    ErrorLogger: Centralized error logging with context and recovery suggestions
    ErrorRecovery: Automated error recovery and retry mechanisms with backoff
    ErrorAnalyzer: Error pattern analysis and reporting for proactive monitoring

Key Features:
    - Detailed error context capture (stack traces, metadata)
    - Recovery suggestion tracking
    - Automated retry with exponential backoff
    - Error pattern detection
    - Integration with process logging

Example:
    >>> from logs.error_handler import ErrorLogger, ErrorRecovery
    >>> import traceback
    >>> 
    >>> # Error logging
    >>> error_logger = ErrorLogger(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='pwd',
    ...     database='warehouse'
    ... )
    >>> 
    >>> try:
    ...     # ... some operation that fails ...
    ...     raise ValueError("Data validation failed")
    ... except Exception as e:
    ...     error_id = error_logger.log_error(
    ...         process_log_id=123,
    ...         error_message=str(e),
    ...         error_detail=traceback.format_exc(),
    ...         table_name="bronze.customer_data",
    ...         recovery_suggestion="Check source data format and constraints"
    ...     )
    >>> 
    >>> # Error recovery with retry
    >>> recovery = ErrorRecovery()
    >>> success = recovery.retry_with_backoff(
    ...     func=risky_operation,
    ...     max_retries=3,
    ...     backoff_factor=2.0
    ... )
"""

import json
import logging
import os

# Import the logging infrastructure models
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Import ORM models from centralized models package (prevents circular imports)
from models.logs_models import ErrorLog, ProcessLog

logger = logging.getLogger(__name__)


class ErrorHandlerError(Exception):
    """Exception raised for error handler operation errors.
    
    Raised when error logging, recovery, or database operations fail.
    """
    pass


class ErrorLogger:
    """Centralized error logging with context and recovery suggestions.
    
    Provides detailed error logging capabilities that integrate with the
    process logging infrastructure. Captures error context, stack traces,
    and recovery suggestions for debugging and automated recovery.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username
        password: Database password
        database: Database name
    
    Example:
        >>> logger = ErrorLogger(
        ...     host='localhost',
        ...     user='postgres',
        ...     password='pwd',
        ...     database='warehouse'
        ... )
        >>> 
        >>> error_id = logger.log_error(
        ...     process_log_id=123,
        ...     error_message="Connection timeout",
        ...     error_detail=traceback.format_exc(),
        ...     table_name="bronze.sales",
        ...     recovery_suggestion="Increase timeout or check network"
        ... )
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the error logger with connection parameters.
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port number
            user: Database username
            password: Database password
            database: Database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
    
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
        if self._engine is None:
            connection_string = (
                f"postgresql://{self.user}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    def _get_session(self) -> Session:
        """Get SQLAlchemy session."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self._get_engine())
        return self._session_factory()
    
    def log_error(
        self,
        process_log_id: int,
        error_message: str,
        error_level: str = 'ERROR',
        error_code: str = None,
        error_detail: str = None,
        table_name: str = None,
        column_name: str = None,
        row_context: str = None,
        recovery_suggestion: str = None,
        exception: Exception = None
    ) -> int:
        """
        Log an error with detailed context.
        
        Args:
            process_log_id: Associated process log ID
            error_message: Human-readable error message
            error_level: Error severity level
            error_code: Application-specific error code
            error_detail: Detailed error information
            table_name: Table involved in the error
            column_name: Column involved in the error
            row_context: Row data context
            recovery_suggestion: Suggested resolution steps
            exception: Python exception object (for automatic detail extraction)
            
        Returns:
            Error log ID
        """
        try:
            session = self._get_session()
            
            # Extract details from exception if provided
            if exception and not error_detail:
                error_detail = traceback.format_exc()
            
            # Auto-generate error code if not provided
            if not error_code and exception:
                error_code = f"{type(exception).__name__}"
            
            error_log = ErrorLog(
                process_log_id=process_log_id,
                error_level=error_level,
                error_code=error_code,
                error_message=error_message,
                error_detail=error_detail,
                table_name=table_name,
                column_name=column_name,
                row_context=row_context,
                recovery_suggestion=recovery_suggestion
            )
            
            session.add(error_log)
            session.commit()
            
            error_id = error_log.error_id
            session.close()
            
            logger.error(f"Logged error {error_id}: {error_message}")
            return error_id
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to log error: {e}")
            raise ErrorHandlerError(f"Failed to log error: {e}")
    
    def log_exception(
        self,
        process_log_id: int,
        exception: Exception,
        context: Dict[str, Any] = None,
        recovery_suggestion: str = None
    ) -> int:
        """
        Log a Python exception with context.
        
        Args:
            process_log_id: Associated process log ID
            exception: Python exception to log
            context: Additional context information
            recovery_suggestion: Suggested resolution steps
            
        Returns:
            Error log ID
        """
        error_message = str(exception)
        error_detail = traceback.format_exc()
        error_code = type(exception).__name__
        
        # Extract context information
        table_name = context.get('table_name') if context else None
        column_name = context.get('column_name') if context else None
        row_context = json.dumps(context) if context else None
        
        return self.log_error(
            process_log_id=process_log_id,
            error_message=error_message,
            error_code=error_code,
            error_detail=error_detail,
            table_name=table_name,
            column_name=column_name,
            row_context=row_context,
            recovery_suggestion=recovery_suggestion
        )
    
    def mark_error_resolved(
        self,
        error_id: int,
        resolved_by: str,
        resolution_notes: str = None
    ) -> None:
        """
        Mark an error as resolved.
        
        Args:
            error_id: Error log ID
            resolved_by: User who resolved the error
            resolution_notes: Optional resolution notes
        """
        try:
            session = self._get_session()
            
            error_log = session.query(ErrorLog).filter_by(error_id=error_id).first()
            if not error_log:
                raise ErrorHandlerError(f"Error log with ID {error_id} not found")
            
            error_log.is_resolved = True
            error_log.resolved_by = resolved_by
            error_log.resolved_timestamp = datetime.now()
            
            if resolution_notes:
                # Store resolution notes in recovery_suggestion field if empty
                if not error_log.recovery_suggestion:
                    error_log.recovery_suggestion = resolution_notes
                else:
                    error_log.recovery_suggestion += f"\n\nResolution: {resolution_notes}"
            
            session.commit()
            session.close()
            
            logger.info(f"Marked error {error_id} as resolved by {resolved_by}")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to mark error as resolved: {e}")
            raise ErrorHandlerError(f"Failed to mark error as resolved: {e}")
    
    def get_unresolved_errors(
        self,
        process_name: str = None,
        error_level: str = None,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get list of unresolved errors.
        
        Args:
            process_name: Filter by process name
            error_level: Filter by error level
            days: Number of days to look back
            
        Returns:
            List of unresolved error records
        """
        try:
            session = self._get_session()
            
            query = session.query(ErrorLog).filter(
                ErrorLog.is_resolved == False,
                ErrorLog.error_timestamp >= datetime.now() - timedelta(days=days)
            )
            
            if error_level:
                query = query.filter(ErrorLog.error_level == error_level)
            
            if process_name:
                query = query.join(ProcessLog).filter(ProcessLog.process_name == process_name)
            
            errors = query.order_by(ErrorLog.error_timestamp.desc()).all()
            
            results = []
            for error in errors:
                results.append({
                    'error_id': error.error_id,
                    'process_log_id': error.process_log_id,
                    'error_timestamp': error.error_timestamp,
                    'error_level': error.error_level,
                    'error_code': error.error_code,
                    'error_message': error.error_message,
                    'table_name': error.table_name,
                    'column_name': error.column_name,
                    'recovery_suggestion': error.recovery_suggestion
                })
            
            session.close()
            return results
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get unresolved errors: {e}")
            raise ErrorHandlerError(f"Failed to get unresolved errors: {e}")


class ErrorRecovery:
    """
    Automated error recovery and retry mechanisms.
    
    Provides utilities for implementing retry logic, circuit breakers,
    and automated recovery procedures.
    """
    
    def __init__(
        self,
        error_logger: ErrorLogger,
        max_retries: int = 3,
        base_delay: float = 1.0,
        backoff_multiplier: float = 2.0
    ):
        """
        Initialize error recovery handler.
        
        Args:
            error_logger: ErrorLogger instance
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries (seconds)
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.error_logger = error_logger
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_multiplier = backoff_multiplier
    
    def retry_with_backoff(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        process_log_id: int = None,
        retryable_exceptions: tuple = (Exception,),
        context: Dict[str, Any] = None
    ) -> Any:
        """
        Retry a function with exponential backoff.
        
        Args:
            func: Function to retry
            args: Function arguments
            kwargs: Function keyword arguments
            process_log_id: Process log ID for error logging
            retryable_exceptions: Tuple of exceptions that should trigger retry
            context: Context information for error logging
            
        Returns:
            Function result if successful
            
        Raises:
            Last exception if all retries failed
        """
        if kwargs is None:
            kwargs = {}
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                
                # Log successful recovery if this wasn't the first attempt
                if attempt > 0:
                    logger.info(f"Function succeeded after {attempt} retries")
                
                return result
                
            except retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    delay = self.base_delay * (self.backoff_multiplier ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s: {e}")
                    
                    # Log the retry attempt
                    if process_log_id and self.error_logger:
                        self.error_logger.log_error(
                            process_log_id=process_log_id,
                            error_message=f"Retry attempt {attempt + 1}: {str(e)}",
                            error_level='WARNING',
                            error_code=f"RETRY_{type(e).__name__}",
                            error_detail=traceback.format_exc(),
                            recovery_suggestion=f"Automatic retry in {delay:.2f}s"
                        )
                    
                    time.sleep(delay)
                else:
                    # Final failure
                    logger.error(f"All {self.max_retries + 1} attempts failed")
                    
                    if process_log_id and self.error_logger:
                        self.error_logger.log_error(
                            process_log_id=process_log_id,
                            error_message=f"Function failed after {self.max_retries + 1} attempts: {str(e)}",
                            error_level='ERROR',
                            error_code=f"RETRY_EXHAUSTED_{type(e).__name__}",
                            error_detail=traceback.format_exc(),
                            recovery_suggestion="Manual intervention required"
                        )
        
        # Re-raise the last exception
        raise last_exception
    
    def circuit_breaker(
        self,
        func: Callable,
        failure_threshold: int = 5,
        timeout: int = 60,
        process_log_id: int = None
    ) -> Any:
        """
        Implement circuit breaker pattern.
        
        Args:
            func: Function to protect with circuit breaker
            failure_threshold: Number of failures before opening circuit
            timeout: Timeout before attempting to close circuit (seconds)
            process_log_id: Process log ID for error logging
            
        Returns:
            Function result or raises exception
        """
        # This is a simplified implementation
        # In production, you'd want to store circuit state persistently
        
        circuit_key = f"{func.__name__}_{id(func)}"
        
        # Check if circuit is open (would need persistent storage in real implementation)
        # For now, just call the function
        try:
            return func()
        except Exception as e:
            if process_log_id and self.error_logger:
                self.error_logger.log_error(
                    process_log_id=process_log_id,
                    error_message=f"Circuit breaker triggered for {func.__name__}: {str(e)}",
                    error_level='WARNING',
                    error_code=f"CIRCUIT_BREAKER_{type(e).__name__}",
                    recovery_suggestion="Circuit breaker will retry after timeout"
                )
            raise


class ErrorAnalyzer:
    """
    Error pattern analysis and reporting.
    
    Provides capabilities to analyze error patterns, identify
    trends, and generate error reports.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the error analyzer."""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
    
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
        if self._engine is None:
            connection_string = (
                f"postgresql://{self.user}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    def analyze_error_patterns(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze error patterns over the specified period.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with error analysis results
        """
        try:
            engine = self._get_engine()
            
            # Error frequency by type
            error_frequency_sql = f"""
            SELECT 
                error_code,
                error_level,
                COUNT(*) as error_count,
                COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_count,
                ROUND(AVG(EXTRACT(EPOCH FROM (resolved_timestamp - error_timestamp))/3600), 2) as avg_resolution_hours
            FROM logs.error_log
            WHERE error_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY error_code, error_level
            ORDER BY error_count DESC
            """
            
            # Error trends by day
            error_trends_sql = f"""
            SELECT 
                DATE(error_timestamp) as error_date,
                error_level,
                COUNT(*) as daily_count
            FROM logs.error_log
            WHERE error_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY DATE(error_timestamp), error_level
            ORDER BY error_date DESC, error_level
            """
            
            # Most problematic tables
            problematic_tables_sql = f"""
            SELECT 
                table_name,
                COUNT(*) as error_count,
                COUNT(DISTINCT error_code) as unique_error_types
            FROM logs.error_log
            WHERE error_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
                AND table_name IS NOT NULL
            GROUP BY table_name
            ORDER BY error_count DESC
            LIMIT 10
            """
            
            with engine.connect() as conn:
                # Execute queries
                error_frequency = conn.execute(text(error_frequency_sql)).fetchall()
                error_trends = conn.execute(text(error_trends_sql)).fetchall()
                problematic_tables = conn.execute(text(problematic_tables_sql)).fetchall()
            
            # Format results
            analysis = {
                'period_days': days,
                'error_frequency': [dict(row._mapping) for row in error_frequency],
                'error_trends': [dict(row._mapping) for row in error_trends],
                'problematic_tables': [dict(row._mapping) for row in problematic_tables],
                'summary': {
                    'total_errors': sum(row[2] for row in error_frequency),
                    'unique_error_types': len(error_frequency),
                    'resolution_rate': (
                        sum(row[3] for row in error_frequency) / 
                        sum(row[2] for row in error_frequency) * 100
                        if sum(row[2] for row in error_frequency) > 0 else 0
                    )
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze error patterns: {e}")
            raise ErrorHandlerError(f"Failed to analyze error patterns: {e}")
    
    def generate_error_report(self, days: int = 7) -> str:
        """
        Generate a formatted error report.
        
        Args:
            days: Number of days to include in report
            
        Returns:
            Formatted error report as string
        """
        try:
            analysis = self.analyze_error_patterns(days)
            
            report = f"""
ERROR ANALYSIS REPORT
=====================
Period: Last {days} days
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY
-------
Total Errors: {analysis['summary']['total_errors']}
Unique Error Types: {analysis['summary']['unique_error_types']}
Resolution Rate: {analysis['summary']['resolution_rate']:.1f}%

TOP ERROR TYPES
---------------
"""
            
            for error in analysis['error_frequency'][:5]:
                report += f"- {error['error_code']} ({error['error_level']}): {error['error_count']} occurrences\n"
            
            report += f"""
MOST PROBLEMATIC TABLES
-----------------------
"""
            
            for table in analysis['problematic_tables'][:5]:
                report += f"- {table['table_name']}: {table['error_count']} errors ({table['unique_error_types']} types)\n"
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate error report: {e}")
            return f"Error generating report: {e}"