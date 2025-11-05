"""
================================================================
Performance monitoring and metrics collection for ETL processes.
================================================================

Provides comprehensive performance monitoring capabilities for the data warehouse
including execution time tracking, resource usage monitoring, and custom metrics
collection for optimization and capacity planning.

Performance monitoring is essential for:
    - Query and process optimization
    - Capacity planning and scaling decisions
    - SLA monitoring and alerting
    - Resource utilization tracking
    - Performance trend analysis

Classes:
    PerformanceMonitor: Track performance metrics for processes
    MetricsCollector: Collect and aggregate system metrics
    ThroughputAnalyzer: Analyze data processing throughput and bottlenecks

Key Features:
    - Execution time tracking
    - Resource usage monitoring (CPU, memory)
    - Custom metric recording
    - Context manager for automatic timing
    - Integration with process logging

Example:
    >>> from logs.performance_monitor import PerformanceMonitor, MetricsCollector
    >>> 
    >>> # Performance monitoring
    >>> monitor = PerformanceMonitor(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='pwd',
    ...     database='warehouse'
    ... )
    >>> 
    >>> # Record individual metric
    >>> monitor.record_metric(
    ...     process_log_id=123,
    ...     metric_name='rows_processed',
    ...     metric_value=1000,
    ...     metric_unit='rows'
    ... )
    >>> 
    >>> # Use context manager for automatic timing
    >>> with monitor.monitor_process(process_log_id=123) as pm:
    ...     # ... do work ...
    ...     pm.record_metric('query_time', 2.5, 'seconds')
    ...     pm.record_metric('rows_scanned', 5000, 'rows')
"""

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import psutil
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

# Import ORM models from centralized models package (prevents circular imports)
from models.logs_models import PerformanceMetrics

logger = logging.getLogger(__name__)


class PerformanceMonitorError(Exception):
    """Exception raised for performance monitoring operation errors.
    
    Raised when metric recording, monitoring, or database operations fail.
    """
    pass


class PerformanceMonitor:
    """Performance monitoring for ETL processes.
    
    Provides capabilities to track execution times, resource usage, and
    custom performance metrics for optimization and capacity planning.
    Integrates with logs.performance_metrics table.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username
        password: Database password
        database: Database name
    
    Example:
        >>> monitor = PerformanceMonitor(
        ...     host='localhost',
        ...     user='postgres',
        ...     password='pwd',
        ...     database='warehouse'
        ... )
        >>> 
        >>> # Record metric
        >>> metric_id = monitor.record_metric(
        ...     process_log_id=123,
        ...     metric_name='processing_time',
        ...     metric_value=45.2,
        ...     metric_unit='seconds',
        ...     additional_context='Bronze layer load'
        ... )
        >>> 
        >>> # Use context manager
        >>> with monitor.monitor_process(123) as pm:
        ...     # Work is automatically timed
        ...     pm.record_metric('rows_processed', 1000)
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the performance monitor with connection parameters.
        
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
            # Use modern URL.create() for robust connection string building
            try:
                connection_url = URL.create(
                    drivername='postgresql',
                    username=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database
                )
                self._engine = create_engine(connection_url, echo=False)
            except AttributeError:
                # Fallback for older SQLAlchemy versions
                connection_string = (
                    f"postgresql://{self.user}:{quote_plus(self.password)}"
                    f"@{self.host}:{self.port}/{self.database}"
                )
                self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    @contextmanager
    def _get_session(self):
        """Get SQLAlchemy session with proper resource management."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self._get_engine())
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def record_metric(
        self,
        process_log_id: int,
        metric_name: str,
        metric_value: float,
        metric_unit: str = None,
        additional_context: str = None
    ) -> int:
        """
        Record a performance metric.
        
        Args:
            process_log_id: Associated process log ID
            metric_name: Name of the metric
            metric_value: Numeric value
            metric_unit: Unit of measurement
            additional_context: Additional context
            
        Returns:
            Metric ID
        """
        try:
            with self._get_session() as session:
                metric = PerformanceMetrics(
                    process_log_id=process_log_id,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    metric_unit=metric_unit,
                    additional_context=additional_context
                )
                
                session.add(metric)
                session.flush()  # Get the ID before commit
                
                metric_id = metric.metric_id
                
                logger.debug(f"Recorded metric '{metric_name}': {metric_value} {metric_unit or ''}")
                return metric_id
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to record metric: {e}")
            raise PerformanceMonitorError(f"Failed to record metric: {e}")
    
    @contextmanager
    def monitor_process(self, process_log_id: int):
        """
        Context manager for monitoring process performance.
        
        Args:
            process_log_id: Process log ID to associate metrics with
            
        Yields:
            ProcessMonitor instance for recording metrics
        """
        monitor = ProcessMonitor(self, process_log_id)
        monitor.start()
        try:
            yield monitor
        finally:
            monitor.end()


class ProcessMonitor:
    """
    Individual process monitor for tracking metrics during execution.
    
    Used as part of the PerformanceMonitor context manager.
    """
    
    def __init__(self, performance_monitor: PerformanceMonitor, process_log_id: int):
        """Initialize process monitor."""
        self.performance_monitor = performance_monitor
        self.process_log_id = process_log_id
        self.start_time = None
        self.start_cpu_times = None
        self.start_memory = None
        
    def start(self):
        """Start monitoring."""
        self.start_time = time.time()
        
        try:
            # Get initial system metrics
            process = psutil.Process()
            self.start_cpu_times = process.cpu_times()
            self.start_memory = process.memory_info()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.warning("Could not access process metrics")
    
    def end(self):
        """End monitoring and record final metrics."""
        if self.start_time is None:
            return
        
        end_time = time.time()
        execution_time = end_time - self.start_time
        
        # Record execution time
        self.performance_monitor.record_metric(
            process_log_id=self.process_log_id,
            metric_name='execution_time',
            metric_value=execution_time,
            metric_unit='seconds'
        )
        
        try:
            # Record resource usage metrics
            process = psutil.Process()
            end_cpu_times = process.cpu_times()
            end_memory = process.memory_info()
            
            if self.start_cpu_times:
                cpu_usage = (
                    (end_cpu_times.user - self.start_cpu_times.user) +
                    (end_cpu_times.system - self.start_cpu_times.system)
                )
                self.performance_monitor.record_metric(
                    process_log_id=self.process_log_id,
                    metric_name='cpu_time',
                    metric_value=cpu_usage,
                    metric_unit='seconds'
                )
            
            if self.start_memory:
                memory_delta = end_memory.rss - self.start_memory.rss
                self.performance_monitor.record_metric(
                    process_log_id=self.process_log_id,
                    metric_name='memory_delta',
                    metric_value=memory_delta / 1024 / 1024,  # Convert to MB
                    metric_unit='MB'
                )
                
                peak_memory = max(end_memory.rss, self.start_memory.rss)
                self.performance_monitor.record_metric(
                    process_log_id=self.process_log_id,
                    metric_name='peak_memory',
                    metric_value=peak_memory / 1024 / 1024,  # Convert to MB
                    metric_unit='MB'
                )
        
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            logger.warning("Could not collect final process metrics")
    
    def record_metric(self, metric_name: str, metric_value: float, metric_unit: str = None):
        """Record a custom metric during process execution."""
        self.performance_monitor.record_metric(
            process_log_id=self.process_log_id,
            metric_name=metric_name,
            metric_value=metric_value,
            metric_unit=metric_unit
        )


class MetricsCollector:
    """
    System-wide metrics collection and aggregation.
    
    Provides capabilities to collect and analyze metrics across
    multiple processes and time periods.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the metrics collector."""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
    
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
        if self._engine is None:
            # Use modern URL.create() for robust connection string building
            try:
                connection_url = URL.create(
                    drivername='postgresql',
                    username=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database
                )
                self._engine = create_engine(connection_url, echo=False)
            except AttributeError:
                # Fallback for older SQLAlchemy versions
                connection_string = (
                    f"postgresql://{self.user}:{quote_plus(self.password)}"
                    f"@{self.host}:{self.port}/{self.database}"
                )
                self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    def get_performance_summary(
        self,
        process_name: str = None,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get performance summary for processes.
        
        Args:
            process_name: Filter by process name
            days: Number of days to analyze
            
        Returns:
            Performance summary dictionary
        """
        try:
            engine = self._get_engine()
            
            # Build the base query
            base_filter = f"pm.measurement_timestamp >= CURRENT_DATE - INTERVAL '{days} days'"
            process_filter = ""
            if process_name:
                process_filter = f"AND pl.process_name = '{process_name}'"
            
            summary_sql = f"""
            SELECT 
                pl.process_name,
                pm.metric_name,
                COUNT(*) as measurement_count,
                AVG(pm.metric_value) as avg_value,
                MIN(pm.metric_value) as min_value,
                MAX(pm.metric_value) as max_value,
                STDDEV(pm.metric_value) as std_dev,
                pm.metric_unit
            FROM logs.performance_metrics pm
            JOIN logs.process_log pl ON pm.process_log_id = pl.log_id
            WHERE {base_filter}
                {process_filter}
            GROUP BY pl.process_name, pm.metric_name, pm.metric_unit
            ORDER BY pl.process_name, pm.metric_name
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(summary_sql)).fetchall()
            
            # Organize results by process
            summary = {}
            for row in results:
                process_name = row[0]
                if process_name not in summary:
                    summary[process_name] = {}
                
                summary[process_name][row[1]] = {
                    'measurement_count': row[2],
                    'avg_value': float(row[3]) if row[3] else 0,
                    'min_value': float(row[4]) if row[4] else 0,
                    'max_value': float(row[5]) if row[5] else 0,
                    'std_dev': float(row[6]) if row[6] else 0,
                    'metric_unit': row[7]
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get performance summary: {e}")
            raise PerformanceMonitorError(f"Failed to get performance summary: {e}")
    
    def get_throughput_analysis(
        self,
        process_name: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Analyze throughput trends for a specific process.
        
        Args:
            process_name: Process name to analyze
            days: Number of days to analyze
            
        Returns:
            Throughput analysis results
        """
        try:
            engine = self._get_engine()
            
            throughput_sql = f"""
            WITH daily_throughput AS (
                SELECT 
                    DATE(pl.start_time) as process_date,
                    COUNT(*) as executions,
                    AVG(pl.rows_processed) as avg_rows_processed,
                    SUM(pl.rows_processed) as total_rows_processed,
                    AVG(EXTRACT(EPOCH FROM (pl.end_time - pl.start_time))) as avg_execution_time
                FROM logs.process_log pl
                WHERE pl.process_name = '{process_name}'
                    AND pl.start_time >= CURRENT_DATE - INTERVAL '{days} days'
                    AND pl.status = 'SUCCESS'
                    AND pl.end_time IS NOT NULL
                GROUP BY DATE(pl.start_time)
                ORDER BY process_date
            )
            SELECT 
                process_date,
                executions,
                avg_rows_processed,
                total_rows_processed,
                avg_execution_time,
                CASE 
                    WHEN avg_execution_time > 0 THEN avg_rows_processed / avg_execution_time
                    ELSE 0
                END as rows_per_second
            FROM daily_throughput
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(throughput_sql)).fetchall()
            
            # Format results
            throughput_data = []
            for row in results:
                throughput_data.append({
                    'date': row[0],
                    'executions': row[1],
                    'avg_rows_processed': float(row[2]) if row[2] else 0,
                    'total_rows_processed': int(row[3]) if row[3] else 0,
                    'avg_execution_time': float(row[4]) if row[4] else 0,
                    'rows_per_second': float(row[5]) if row[5] else 0
                })
            
            # Calculate overall statistics
            if throughput_data:
                total_executions = sum(d['executions'] for d in throughput_data)
                avg_throughput = sum(d['rows_per_second'] for d in throughput_data) / len(throughput_data)
                total_rows = sum(d['total_rows_processed'] for d in throughput_data)
            else:
                total_executions = 0
                avg_throughput = 0
                total_rows = 0
            
            return {
                'process_name': process_name,
                'analysis_period_days': days,
                'daily_data': throughput_data,
                'summary': {
                    'total_executions': total_executions,
                    'average_throughput_rows_per_second': avg_throughput,
                    'total_rows_processed': total_rows
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze throughput: {e}")
            raise PerformanceMonitorError(f"Failed to analyze throughput: {e}")


class ThroughputAnalyzer:
    """
    Specialized analyzer for data processing throughput.
    
    Provides detailed analysis of data processing performance
    and identifies bottlenecks.
    """
    
    def __init__(self, metrics_collector: MetricsCollector):
        """Initialize throughput analyzer."""
        self.metrics_collector = metrics_collector
    
    def identify_bottlenecks(
        self,
        process_name: str,
        threshold_percentile: float = 95.0
    ) -> Dict[str, Any]:
        """
        Identify performance bottlenecks for a process.
        
        Args:
            process_name: Process name to analyze
            threshold_percentile: Percentile threshold for identifying slow executions
            
        Returns:
            Bottleneck analysis results
        """
        try:
            engine = self.metrics_collector._get_engine()
            
            bottleneck_sql = f"""
            WITH execution_stats AS (
                SELECT 
                    pl.log_id,
                    pl.start_time,
                    EXTRACT(EPOCH FROM (pl.end_time - pl.start_time)) as execution_time,
                    pl.rows_processed,
                    CASE 
                        WHEN EXTRACT(EPOCH FROM (pl.end_time - pl.start_time)) > 0 
                        THEN pl.rows_processed / EXTRACT(EPOCH FROM (pl.end_time - pl.start_time))
                        ELSE 0
                    END as throughput_rows_per_second
                FROM logs.process_log pl
                WHERE pl.process_name = '{process_name}'
                    AND pl.status = 'SUCCESS'
                    AND pl.end_time IS NOT NULL
                    AND pl.rows_processed IS NOT NULL
                    AND pl.start_time >= CURRENT_DATE - INTERVAL '30 days'
            ),
            percentiles AS (
                SELECT 
                    PERCENTILE_CONT({threshold_percentile/100}) WITHIN GROUP (ORDER BY execution_time) as time_threshold,
                    PERCENTILE_CONT({(100-threshold_percentile)/100}) WITHIN GROUP (ORDER BY throughput_rows_per_second) as throughput_threshold
                FROM execution_stats
            )
            SELECT 
                es.log_id,
                es.start_time,
                es.execution_time,
                es.rows_processed,
                es.throughput_rows_per_second,
                CASE 
                    WHEN es.execution_time > p.time_threshold THEN 'SLOW_EXECUTION'
                    WHEN es.throughput_rows_per_second < p.throughput_threshold THEN 'LOW_THROUGHPUT'
                    ELSE 'NORMAL'
                END as performance_category
            FROM execution_stats es
            CROSS JOIN percentiles p
            WHERE es.execution_time > p.time_threshold 
                OR es.throughput_rows_per_second < p.throughput_threshold
            ORDER BY es.execution_time DESC
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(bottleneck_sql)).fetchall()
            
            bottlenecks = []
            for row in results:
                bottlenecks.append({
                    'log_id': row[0],
                    'start_time': row[1],
                    'execution_time': float(row[2]),
                    'rows_processed': row[3],
                    'throughput_rows_per_second': float(row[4]),
                    'performance_category': row[5]
                })
            
            return {
                'process_name': process_name,
                'threshold_percentile': threshold_percentile,
                'bottlenecks_found': len(bottlenecks),
                'bottlenecks': bottlenecks
            }
            
        except Exception as e:
            logger.error(f"Failed to identify bottlenecks: {e}")
            raise PerformanceMonitorError(f"Failed to identify bottlenecks: {e}")