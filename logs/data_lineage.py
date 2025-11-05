"""
==================================================================
Data lineage tracking and analysis for the medallion architecture.
==================================================================

Provides comprehensive data lineage tracking capabilities that trace data flow
through bronze, silver, and gold layers with detailed transformation tracking.

Data lineage is critical for:
    - Regulatory compliance and audit trails
    - Impact analysis for schema changes
    - Data quality root cause analysis
    - Documentation and data governance

Classes:
    LineageTracker: Track data lineage through transformations
    LineageAnalyzer: Analyze and visualize data lineage relationships
    ImpactAnalyzer: Assess downstream impact of changes

Key Features:
    - Complete lineage tracking across medallion layers
    - Transformation logic documentation
    - Source-to-target mapping
    - Impact analysis for changes
    - Lineage visualization support

Example:
    >>> from logs.data_lineage import LineageTracker, LineageAnalyzer
    >>> 
    >>> # Track lineage
    >>> tracker = LineageTracker(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='pwd',
    ...     database='warehouse'
    ... )
    >>> 
    >>> lineage_id = tracker.log_lineage(
    ...     process_log_id=123,
    ...     source_schema='bronze',
    ...     source_table='crm_customers',
    ...     target_schema='silver',
    ...     target_table='customers',
    ...     transformation_logic='Data cleansing and standardization',
    ...     rows_read=1000,
    ...     rows_written=950
    ... )
    >>> 
    >>> # Analyze lineage
    >>> analyzer = LineageAnalyzer()
    >>> lineage_map = analyzer.get_lineage_map('silver', 'customers')
    >>> upstream = analyzer.get_upstream_dependencies('gold', 'customer_analytics')
"""

import logging
import os

# Import the logging infrastructure models
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Import ORM models from centralized models package (prevents circular imports)
from models.logs_models import DataLineage

logger = logging.getLogger(__name__)


class DataLineageError(Exception):
    """Exception raised for data lineage operation errors.
    
    Raised when lineage tracking, analysis, or database operations fail.
    """
    pass


class LineageTracker:
    """Data lineage tracking for the medallion architecture.
    
    Provides capabilities to track data flow through bronze, silver, and
    gold layers with detailed transformation logic, source-to-target mapping,
    and row count tracking for data quality monitoring.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username
        password: Database password
        database: Database name
    
    Example:
        >>> tracker = LineageTracker(
        ...     host='localhost',
        ...     user='postgres',
        ...     password='pwd',
        ...     database='warehouse'
        ... )
        >>> 
        >>> lineage_id = tracker.log_lineage(
        ...     process_log_id=123,
        ...     source_schema='bronze',
        ...     source_table='raw_customers',
        ...     target_schema='silver',
        ...     target_table='customers',
        ...     transformation_logic='Cleansing and deduplication',
        ...     rows_read=1000,
        ...     rows_written=950
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
        """Initialize the lineage tracker with connection parameters.
        
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
    
    def log_lineage(
        self,
        process_log_id: int,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        source_column: str = None,
        target_column: str = None,
        transformation_logic: str = None,
        record_count: int = None
    ) -> int:
        """
        Log data lineage information.
        
        Args:
            process_log_id: Associated process log ID
            source_schema: Source schema name
            source_table: Source table name
            target_schema: Target schema name
            target_table: Target table name
            source_column: Source column name (optional)
            target_column: Target column name (optional)
            transformation_logic: Description of transformation
            record_count: Number of records involved
            
        Returns:
            Data lineage ID
        """
        try:
            session = self._get_session()
            
            lineage = DataLineage(
                process_log_id=process_log_id,
                source_schema=source_schema,
                source_table=source_table,
                source_column=source_column,
                target_schema=target_schema,
                target_table=target_table,
                target_column=target_column,
                transformation_logic=transformation_logic,
                record_count=record_count
            )
            
            session.add(lineage)
            session.commit()
            
            lineage_id = lineage.lineage_id
            session.close()
            
            logger.info(f"Logged lineage {lineage_id}: {source_schema}.{source_table} -> {target_schema}.{target_table}")
            return lineage_id
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to log data lineage: {e}")
            raise DataLineageError(f"Failed to log data lineage: {e}")
    
    def log_table_lineage(
        self,
        process_log_id: int,
        source_tables: List[Tuple[str, str]],  # List of (schema, table) tuples
        target_table: Tuple[str, str],  # (schema, table) tuple
        transformation_logic: str = None,
        record_count: int = None
    ) -> List[int]:
        """
        Log lineage for multiple source tables to one target table.
        
        Args:
            process_log_id: Associated process log ID
            source_tables: List of (source_schema, source_table) tuples
            target_table: (target_schema, target_table) tuple
            transformation_logic: Description of transformation
            record_count: Number of records involved
            
        Returns:
            List of lineage IDs
        """
        lineage_ids = []
        
        for source_schema, source_table in source_tables:
            lineage_id = self.log_lineage(
                process_log_id=process_log_id,
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_table[0],
                target_table=target_table[1],
                transformation_logic=transformation_logic,
                record_count=record_count
            )
            lineage_ids.append(lineage_id)
        
        return lineage_ids
    
    def log_column_lineage(
        self,
        process_log_id: int,
        column_mappings: List[Dict[str, str]],
        transformation_logic: str = None
    ) -> List[int]:
        """
        Log detailed column-level lineage.
        
        Args:
            process_log_id: Associated process log ID
            column_mappings: List of column mapping dictionaries
                Each dict should have: source_schema, source_table, source_column,
                target_schema, target_table, target_column, transformation
            transformation_logic: Overall transformation description
            
        Returns:
            List of lineage IDs
        """
        lineage_ids = []
        
        for mapping in column_mappings:
            column_transformation = mapping.get('transformation', transformation_logic)
            
            lineage_id = self.log_lineage(
                process_log_id=process_log_id,
                source_schema=mapping['source_schema'],
                source_table=mapping['source_table'],
                source_column=mapping['source_column'],
                target_schema=mapping['target_schema'],
                target_table=mapping['target_table'],
                target_column=mapping['target_column'],
                transformation_logic=column_transformation
            )
            lineage_ids.append(lineage_id)
        
        return lineage_ids


class LineageAnalyzer:
    """
    Data lineage analysis and visualization.
    
    Provides capabilities to analyze lineage relationships,
    trace data flow, and generate lineage reports.
    """
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the lineage analyzer."""
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
    
    def get_upstream_lineage(
        self,
        target_schema: str,
        target_table: str,
        max_depth: int = 5
    ) -> Dict[str, Any]:
        """
        Get upstream lineage for a table (what feeds into it).
        
        Args:
            target_schema: Target schema name
            target_table: Target table name
            max_depth: Maximum depth to traverse
            
        Returns:
            Upstream lineage information
        """
        try:
            engine = self._get_engine()
            
            upstream_sql = f"""
            WITH RECURSIVE upstream_lineage AS (
                -- Base case: direct upstream tables
                SELECT 
                    dl.lineage_id,
                    dl.source_schema,
                    dl.source_table,
                    dl.source_column,
                    dl.target_schema,
                    dl.target_table,
                    dl.target_column,
                    dl.transformation_logic,
                    dl.record_count,
                    dl.created_timestamp,
                    1 as depth,
                    ARRAY[dl.source_schema || '.' || dl.source_table] as path
                FROM logs.data_lineage dl
                WHERE dl.target_schema = '{target_schema}'
                    AND dl.target_table = '{target_table}'
                
                UNION ALL
                
                -- Recursive case: follow upstream dependencies
                SELECT 
                    dl.lineage_id,
                    dl.source_schema,
                    dl.source_table,
                    dl.source_column,
                    dl.target_schema,
                    dl.target_table,
                    dl.target_column,
                    dl.transformation_logic,
                    dl.record_count,
                    dl.created_timestamp,
                    ul.depth + 1,
                    ul.path || (dl.source_schema || '.' || dl.source_table)
                FROM logs.data_lineage dl
                JOIN upstream_lineage ul ON (
                    dl.target_schema = ul.source_schema 
                    AND dl.target_table = ul.source_table
                )
                WHERE ul.depth < {max_depth}
                    AND NOT (dl.source_schema || '.' || dl.source_table = ANY(ul.path))
            )
            SELECT DISTINCT
                source_schema,
                source_table,
                source_column,
                target_schema,
                target_table,
                target_column,
                transformation_logic,
                record_count,
                depth,
                created_timestamp
            FROM upstream_lineage
            ORDER BY depth, source_schema, source_table
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(upstream_sql)).fetchall()
            
            # Organize results by depth
            lineage_map = {}
            for row in results:
                depth = row[8]
                if depth not in lineage_map:
                    lineage_map[depth] = []
                
                lineage_map[depth].append({
                    'source_schema': row[0],
                    'source_table': row[1],
                    'source_column': row[2],
                    'target_schema': row[3],
                    'target_table': row[4],
                    'target_column': row[5],
                    'transformation_logic': row[6],
                    'record_count': row[7],
                    'created_timestamp': row[9]
                })
            
            return {
                'target_table': f"{target_schema}.{target_table}",
                'max_depth_analyzed': max_depth,
                'lineage_by_depth': lineage_map,
                'total_upstream_tables': len(set(
                    f"{row[0]}.{row[1]}" for row in results
                ))
            }
            
        except Exception as e:
            logger.error(f"Failed to get upstream lineage: {e}")
            raise DataLineageError(f"Failed to get upstream lineage: {e}")
    
    def get_downstream_lineage(
        self,
        source_schema: str,
        source_table: str,
        max_depth: int = 5
    ) -> Dict[str, Any]:
        """
        Get downstream lineage for a table (what it feeds into).
        
        Args:
            source_schema: Source schema name
            source_table: Source table name
            max_depth: Maximum depth to traverse
            
        Returns:
            Downstream lineage information
        """
        try:
            engine = self._get_engine()
            
            downstream_sql = f"""
            WITH RECURSIVE downstream_lineage AS (
                -- Base case: direct downstream tables
                SELECT 
                    dl.lineage_id,
                    dl.source_schema,
                    dl.source_table,
                    dl.source_column,
                    dl.target_schema,
                    dl.target_table,
                    dl.target_column,
                    dl.transformation_logic,
                    dl.record_count,
                    dl.created_timestamp,
                    1 as depth,
                    ARRAY[dl.target_schema || '.' || dl.target_table] as path
                FROM logs.data_lineage dl
                WHERE dl.source_schema = '{source_schema}'
                    AND dl.source_table = '{source_table}'
                
                UNION ALL
                
                -- Recursive case: follow downstream dependencies
                SELECT 
                    dl.lineage_id,
                    dl.source_schema,
                    dl.source_table,
                    dl.source_column,
                    dl.target_schema,
                    dl.target_table,
                    dl.target_column,
                    dl.transformation_logic,
                    dl.record_count,
                    dl.created_timestamp,
                    dl_rec.depth + 1,
                    dl_rec.path || (dl.target_schema || '.' || dl.target_table)
                FROM logs.data_lineage dl
                JOIN downstream_lineage dl_rec ON (
                    dl.source_schema = dl_rec.target_schema 
                    AND dl.source_table = dl_rec.target_table
                )
                WHERE dl_rec.depth < {max_depth}
                    AND NOT (dl.target_schema || '.' || dl.target_table = ANY(dl_rec.path))
            )
            SELECT DISTINCT
                source_schema,
                source_table,
                source_column,
                target_schema,
                target_table,
                target_column,
                transformation_logic,
                record_count,
                depth,
                created_timestamp
            FROM downstream_lineage
            ORDER BY depth, target_schema, target_table
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(downstream_sql)).fetchall()
            
            # Organize results by depth
            lineage_map = {}
            for row in results:
                depth = row[8]
                if depth not in lineage_map:
                    lineage_map[depth] = []
                
                lineage_map[depth].append({
                    'source_schema': row[0],
                    'source_table': row[1],
                    'source_column': row[2],
                    'target_schema': row[3],
                    'target_table': row[4],
                    'target_column': row[5],
                    'transformation_logic': row[6],
                    'record_count': row[7],
                    'created_timestamp': row[9]
                })
            
            return {
                'source_table': f"{source_schema}.{source_table}",
                'max_depth_analyzed': max_depth,
                'lineage_by_depth': lineage_map,
                'total_downstream_tables': len(set(
                    f"{row[3]}.{row[4]}" for row in results
                ))
            }
            
        except Exception as e:
            logger.error(f"Failed to get downstream lineage: {e}")
            raise DataLineageError(f"Failed to get downstream lineage: {e}")
    
    def get_medallion_flow(self) -> Dict[str, Any]:
        """
        Get complete medallion architecture data flow.
        
        Returns:
            Complete medallion flow mapping
        """
        try:
            engine = self._get_engine()
            
            medallion_sql = """
            SELECT 
                dl.source_schema,
                dl.source_table,
                dl.target_schema,
                dl.target_table,
                COUNT(*) as transformation_count,
                COUNT(DISTINCT dl.transformation_logic) as unique_transformations,
                MAX(dl.created_timestamp) as latest_update,
                SUM(dl.record_count) as total_records_processed
            FROM logs.data_lineage dl
            WHERE dl.source_schema IN ('bronze', 'silver', 'gold')
                AND dl.target_schema IN ('bronze', 'silver', 'gold')
            GROUP BY dl.source_schema, dl.source_table, dl.target_schema, dl.target_table
            ORDER BY 
                CASE dl.source_schema 
                    WHEN 'bronze' THEN 1 
                    WHEN 'silver' THEN 2 
                    WHEN 'gold' THEN 3 
                END,
                CASE dl.target_schema 
                    WHEN 'bronze' THEN 1 
                    WHEN 'silver' THEN 2 
                    WHEN 'gold' THEN 3 
                END,
                dl.source_table, dl.target_table
            """
            
            with engine.connect() as conn:
                results = conn.execute(text(medallion_sql)).fetchall()
            
            # Organize by source layer
            flow_map = {
                'bronze_to_silver': [],
                'silver_to_gold': [],
                'other_flows': []
            }
            
            for row in results:
                flow_info = {
                    'source_table': f"{row[0]}.{row[1]}",
                    'target_table': f"{row[2]}.{row[3]}",
                    'transformation_count': row[4],
                    'unique_transformations': row[5],
                    'latest_update': row[6],
                    'total_records_processed': row[7]
                }
                
                if row[0] == 'bronze' and row[2] == 'silver':
                    flow_map['bronze_to_silver'].append(flow_info)
                elif row[0] == 'silver' and row[2] == 'gold':
                    flow_map['silver_to_gold'].append(flow_info)
                else:
                    flow_map['other_flows'].append(flow_info)
            
            return flow_map
            
        except Exception as e:
            logger.error(f"Failed to get medallion flow: {e}")
            raise DataLineageError(f"Failed to get medallion flow: {e}")


class ImpactAnalyzer:
    """
    Impact analysis for data changes.
    
    Analyzes the potential impact of changes to tables
    on downstream systems and processes.
    """
    
    def __init__(self, lineage_analyzer: LineageAnalyzer):
        """Initialize impact analyzer."""
        self.lineage_analyzer = lineage_analyzer
    
    def analyze_impact(
        self,
        changed_schema: str,
        changed_table: str,
        change_type: str = 'SCHEMA_CHANGE'
    ) -> Dict[str, Any]:
        """
        Analyze impact of changes to a table.
        
        Args:
            changed_schema: Schema of the changed table
            changed_table: Name of the changed table
            change_type: Type of change (SCHEMA_CHANGE, DATA_CHANGE, etc.)
            
        Returns:
            Impact analysis results
        """
        try:
            # Get downstream lineage
            downstream = self.lineage_analyzer.get_downstream_lineage(
                changed_schema, changed_table
            )
            
            # Analyze impact severity
            impact_severity = 'LOW'
            if downstream['total_downstream_tables'] > 5:
                impact_severity = 'HIGH'
            elif downstream['total_downstream_tables'] > 2:
                impact_severity = 'MEDIUM'
            
            # Identify critical paths (paths to gold layer)
            critical_paths = []
            for depth, lineages in downstream['lineage_by_depth'].items():
                for lineage in lineages:
                    if lineage['target_schema'] == 'gold':
                        critical_paths.append({
                            'target_table': f"{lineage['target_schema']}.{lineage['target_table']}",
                            'depth': depth,
                            'transformation': lineage['transformation_logic']
                        })
            
            return {
                'changed_table': f"{changed_schema}.{changed_table}",
                'change_type': change_type,
                'impact_severity': impact_severity,
                'affected_tables_count': downstream['total_downstream_tables'],
                'max_depth_affected': max(downstream['lineage_by_depth'].keys()) if downstream['lineage_by_depth'] else 0,
                'critical_paths': critical_paths,
                'full_downstream_lineage': downstream,
                'recommendations': self._generate_recommendations(
                    impact_severity, len(critical_paths), downstream['total_downstream_tables']
                )
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze impact: {e}")
            raise DataLineageError(f"Failed to analyze impact: {e}")
    
    def _generate_recommendations(
        self,
        impact_severity: str,
        critical_paths_count: int,
        affected_tables_count: int
    ) -> List[str]:
        """Generate recommendations based on impact analysis."""
        recommendations = []
        
        if impact_severity == 'HIGH':
            recommendations.append("HIGH IMPACT: Coordinate with downstream teams before making changes")
            recommendations.append("Consider implementing change in phases with rollback plan")
        
        if critical_paths_count > 0:
            recommendations.append(f"CRITICAL: {critical_paths_count} gold layer tables affected - validate business logic")
            recommendations.append("Test changes in development environment with full data pipeline")
        
        if affected_tables_count > 3:
            recommendations.append("Multiple tables affected - consider batching notifications")
            recommendations.append("Update data lineage documentation after changes")
        
        if not recommendations:
            recommendations.append("LOW IMPACT: Standard change management processes apply")
        
        return recommendations