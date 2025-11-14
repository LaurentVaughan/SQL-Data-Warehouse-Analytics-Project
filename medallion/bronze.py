"""
=================================================
Bronze Layer Manager for Medallion Architecture
=================================================

Orchestrates raw data ingestion from source systems (CRM/ERP) into the
bronze schema with comprehensive logging, validation, and lineage tracking.

The BronzeManager provides:
    - Automated table creation from CSV schema inference
    - Bulk data loading with batch processing
    - Comprehensive audit logging and lineage tracking
    - Error handling with recovery mechanisms
    - Hash-based change detection for incremental loads

Architecture:
    Source CSV Files → Validation → Bronze Tables → Audit Logs

Key Features:
    - Schema-on-read with minimal transformation
    - Append-only storage pattern
    - Full audit trail for every operation
    - Integration with existing logging infrastructure
    - Uses sql/ utilities for all DDL/DML operations

Example:
    >>> from medallion.bronze import BronzeManager
    >>> 
    >>> # Initialize manager
    >>> manager = BronzeManager()
    >>> 
    >>> # Load CRM customer data
    >>> result = manager.load_csv_to_bronze(
    ...     csv_path='datasets/source_crm/cust_info.csv',
    ...     table_name='crm_customers_raw',
    ...     source_system='CRM'
    ... )
    >>> 
    >>> # Load all CRM data
    >>> manager.load_all_crm_data()
"""

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Core infrastructure
from core.config import config
from core.logger import get_logger

# Logging infrastructure
from logs.audit_logger import ProcessLogger
from logs.data_lineage import LineageTracker
from logs.error_handler import ErrorLogger, ErrorRecovery
from logs.performance_monitor import PerformanceMonitor

# SQL utilities
from sql.ddl import create_table
from sql.dml import bulk_insert
from sql.query_builder import check_table_exists_sql

# Database utilities
from utils.database_utils import create_sqlalchemy_engine, verify_database_exists

logger = get_logger(__name__)


class BronzeManagerError(Exception):
    """Exception raised for bronze layer operation errors."""
    pass


class BronzeManager:
    """
    Orchestrates bronze layer data ingestion and management.
    
    Provides end-to-end functionality for loading raw data from CSV files
    into PostgreSQL bronze tables with full audit trails and lineage tracking.
    
    Attributes:
        engine: SQLAlchemy engine for database operations
        process_logger: ProcessLogger for audit trails
        error_logger: ErrorLogger for error tracking
        lineage_tracker: LineageTracker for data lineage
        perf_monitor: PerformanceMonitor for performance tracking
        error_recovery: ErrorRecovery for retry logic
        bronze_schema: Name of bronze schema (default: 'bronze')
        
    Example:
        >>> manager = BronzeManager()
        >>> result = manager.load_csv_to_bronze(
        ...     csv_path='datasets/source_crm/cust_info.csv',
        ...     table_name='crm_customers_raw'
        ... )
        >>> print(f"Loaded {result['rows_loaded']} rows")
    """
    
    # Bronze metadata column definitions
    BRONZE_METADATA_COLUMNS = {
        '_bronze_id': 'BIGSERIAL PRIMARY KEY',
        '_ingestion_timestamp': 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP',
        '_ingestion_batch_id': 'VARCHAR(100) NOT NULL',
        '_source_file': 'VARCHAR(500) NOT NULL',
        '_source_row_number': 'INTEGER NOT NULL',
        '_is_current': 'BOOLEAN NOT NULL DEFAULT TRUE',
        '_row_hash': 'VARCHAR(64) NOT NULL'
    }
    
    def __init__(
        self,
        monitor_performance: bool = True,
        bronze_schema: str = 'bronze'
    ):
        """
        Initialize BronzeManager with database connection and logging.
        
        Args:
            monitor_performance: Enable performance monitoring
            bronze_schema: Name of bronze schema (default: 'bronze')
            
        Raises:
            BronzeManagerError: If warehouse database doesn't exist
        """
        logger.info("🔧 Initializing BronzeManager...")
        
        self.bronze_schema = bronze_schema
        self.monitor_performance = monitor_performance
        
        # Verify warehouse database exists
        if not verify_database_exists(config.warehouse_db_name):
            raise BronzeManagerError(
                f"Warehouse database '{config.warehouse_db_name}' does not exist. "
                "Run setup first: python main.py --setup"
            )
        
        # Initialize database connection
        try:
            self.engine = create_sqlalchemy_engine(
                use_warehouse=True,
                pool_size=10,
                max_overflow=20
            )
            logger.info(f"✅ Connected to {config.warehouse_db_name}")
        except Exception as e:
            raise BronzeManagerError(f"Failed to connect to database: {e}")
        
        # Initialize logging infrastructure
        try:
            self.process_logger = ProcessLogger(
                host=config.db_host,
                port=config.db_port,
                user=config.db_user,
                password=config.db_password,
                database=config.warehouse_db_name
            )
            
            self.error_logger = ErrorLogger(
                host=config.db_host,
                port=config.db_port,
                user=config.db_user,
                password=config.db_password,
                database=config.warehouse_db_name
            )
            
            self.lineage_tracker = LineageTracker(
                host=config.db_host,
                port=config.db_port,
                user=config.db_user,
                password=config.db_password,
                database=config.warehouse_db_name
            )
            
            if monitor_performance:
                self.perf_monitor = PerformanceMonitor(
                    host=config.db_host,
                    port=config.db_port,
                    user=config.db_user,
                    password=config.db_password,
                    database=config.warehouse_db_name
                )
            else:
                self.perf_monitor = None
            
            self.error_recovery = ErrorRecovery(
                error_logger=self.error_logger,
                max_retries=3,
                base_delay=1.0,
                backoff_multiplier=2.0
            )
            
            logger.info("✅ Logging infrastructure initialized")
            
        except Exception as e:
            raise BronzeManagerError(f"Failed to initialize logging: {e}")
    
    def _infer_sql_type(self, dtype: str) -> str:
        """
        Infer PostgreSQL type from pandas dtype.
        
        Args:
            dtype: Pandas dtype string
            
        Returns:
            PostgreSQL type definition
        """
        dtype_str = str(dtype).lower()
        
        if 'int' in dtype_str:
            return 'BIGINT'
        elif 'float' in dtype_str:
            return 'NUMERIC'
        elif 'bool' in dtype_str:
            return 'BOOLEAN'
        elif 'datetime' in dtype_str:
            return 'TIMESTAMP'
        elif 'date' in dtype_str:
            return 'DATE'
        else:
            return 'TEXT'
    
    def _infer_table_schema(
        self,
        df: pd.DataFrame,
        include_metadata: bool = True
    ) -> Dict[str, str]:
        """
        Infer table schema from DataFrame.
        
        Args:
            df: Source DataFrame
            include_metadata: Include bronze metadata columns
            
        Returns:
            Dictionary mapping column names to SQL types
        """
        schema = {}
        
        # Add source columns
        for col in df.columns:
            schema[col] = self._infer_sql_type(df[col].dtype)
        
        # Add bronze metadata columns
        if include_metadata:
            schema.update(self.BRONZE_METADATA_COLUMNS)
        
        return schema
    
    def _calculate_row_hash(self, row_data: Dict[str, Any]) -> str:
        """
        Calculate SHA256 hash of row data for change detection.
        
        Args:
            row_data: Dictionary of column values
            
        Returns:
            SHA256 hash string
        """
        # Sort keys for consistent hashing
        sorted_items = sorted(row_data.items())
        row_string = str(sorted_items)
        return hashlib.sha256(row_string.encode()).hexdigest()
    
    def _enrich_dataframe(
        self,
        df: pd.DataFrame,
        batch_id: str,
        source_file: str
    ) -> pd.DataFrame:
        """
        Enrich DataFrame with bronze metadata columns.
        
        Args:
            df: Source DataFrame
            batch_id: Unique batch identifier
            source_file: Source file path
            
        Returns:
            DataFrame with metadata columns added
        """
        enriched = df.copy()
        
        # Add metadata columns
        enriched['_ingestion_batch_id'] = batch_id
        enriched['_source_file'] = source_file
        enriched['_source_row_number'] = range(1, len(enriched) + 1)
        enriched['_is_current'] = True
        
        # Calculate row hashes
        enriched['_row_hash'] = enriched.apply(
            lambda row: self._calculate_row_hash(row.to_dict()),
            axis=1
        )
        
        return enriched
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if table exists in bronze schema.
        
        Args:
            table_name: Name of table to check
            schema: Schema name (defaults to bronze_schema)
            
        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.bronze_schema
        
        try:
            with self.engine.connect() as conn:
                sql = check_table_exists_sql(schema, table_name)
                result = conn.execute(text(sql))
                exists = result.fetchone()[0]
                return exists
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def create_bronze_table(
        self,
        table_name: str,
        schema_def: Dict[str, str],
        drop_if_exists: bool = False
    ) -> bool:
        """
        Create bronze table with inferred or provided schema.
        
        Args:
            table_name: Name of table to create
            schema_def: Dictionary mapping column names to SQL types
            drop_if_exists: Drop table if it already exists
            
        Returns:
            True if table created successfully
            
        Raises:
            BronzeManagerError: If table creation fails
        """
        full_table_name = f"{self.bronze_schema}.{table_name}"
        
        try:
            logger.info(f"📊 Creating table {full_table_name}...")
            
            # Check if table exists
            if self.table_exists(table_name):
                if drop_if_exists:
                    logger.warning(f"⚠️  Dropping existing table {full_table_name}")
                    with self.engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE {full_table_name}"))
                        conn.commit()
                else:
                    logger.info(f"ℹ️  Table {full_table_name} already exists")
                    return True
            
            # Generate CREATE TABLE DDL using sql.ddl
            create_ddl = create_table(
                table_name=table_name,
                columns=schema_def,
                schema=self.bronze_schema
            )
            
            # Execute DDL
            with self.engine.connect() as conn:
                conn.execute(text(create_ddl))
                conn.commit()
            
            logger.info(f"✅ Created table {full_table_name}")
            return True
            
        except SQLAlchemyError as e:
            error_msg = f"Failed to create table {full_table_name}: {e}"
            logger.error(error_msg)
            raise BronzeManagerError(error_msg)
    
    def load_csv_to_bronze(
        self,
        csv_path: str,
        table_name: str,
        source_system: str = 'UNKNOWN',
        batch_size: int = 10000,
        create_table: bool = True,
        drop_if_exists: bool = False
    ) -> Dict[str, Any]:
        """
        Load CSV file into bronze table with full audit trail.
        
        This is the main entry point for bronze data ingestion. It handles:
        - CSV reading and validation
        - Table creation (if needed)
        - Data enrichment with metadata
        - Bulk loading with batch processing
        - Audit logging and lineage tracking
        - Error handling with recovery
        
        Args:
            csv_path: Path to CSV file (relative or absolute)
            table_name: Target bronze table name (without schema)
            source_system: Source system identifier (e.g., 'CRM', 'ERP')
            batch_size: Number of rows per batch insert
            create_table: Create table if it doesn't exist
            drop_if_exists: Drop and recreate table
            
        Returns:
            Dictionary with load results:
                - rows_loaded: Number of rows loaded
                - batch_id: Unique batch identifier
                - process_log_id: Process log ID for tracking
                - duration_seconds: Load duration
                - table_name: Full table name
                
        Raises:
            BronzeManagerError: If load fails
            
        Example:
            >>> manager = BronzeManager()
            >>> result = manager.load_csv_to_bronze(
            ...     csv_path='datasets/source_crm/cust_info.csv',
            ...     table_name='crm_customers_raw',
            ...     source_system='CRM'
            ... )
            >>> print(f"Loaded {result['rows_loaded']} rows in {result['duration_seconds']:.2f}s")
        """
        start_time = datetime.now()
        batch_id = f"{source_system}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        
        # Start process logging
        process_log_id = self.process_logger.start_process(
            process_name=f'bronze_ingestion_{table_name}',
            process_description=f'Load {Path(csv_path).name} into {self.bronze_schema}.{table_name}',
            source_system=source_system,
            target_layer='bronze',
            metadata={
                'csv_path': str(csv_path),
                'table_name': table_name,
                'batch_id': batch_id,
                'batch_size': batch_size
            }
        )
        
        logger.info(f"\n{'='*70}")
        logger.info(f"🔵 BRONZE LAYER INGESTION")
        logger.info(f"{'='*70}")
        logger.info(f"📁 Source: {csv_path}")
        logger.info(f"📊 Target: {self.bronze_schema}.{table_name}")
        logger.info(f"🏷️  Batch ID: {batch_id}")
        
        try:
            # 1. Read CSV file
            logger.info(f"📖 Reading CSV file...")
            csv_full_path = Path(csv_path)
            
            if not csv_full_path.exists():
                # Try relative to project root
                csv_full_path = config.project.project_root / csv_path
                
            if not csv_full_path.exists():
                raise BronzeManagerError(f"CSV file not found: {csv_path}")
            
            df = pd.read_csv(csv_full_path)
            total_rows = len(df)
            logger.info(f"✅ Read {total_rows:,} rows from {csv_full_path.name}")
            
            # 2. Create table if needed
            if create_table or not self.table_exists(table_name):
                schema_def = self._infer_table_schema(df, include_metadata=True)
                self.create_bronze_table(
                    table_name=table_name,
                    schema_def=schema_def,
                    drop_if_exists=drop_if_exists
                )
            
            # 3. Enrich with metadata
            logger.info(f"🔧 Enriching data with bronze metadata...")
            enriched_df = self._enrich_dataframe(
                df=df,
                batch_id=batch_id,
                source_file=str(csv_full_path)
            )
            
            # 4. Bulk load data
            logger.info(f"💾 Loading data in batches of {batch_size:,} rows...")
            full_table_name = f"{self.bronze_schema}.{table_name}"
            
            # Use sql.dml.bulk_insert for batch loading
            rows_loaded = 0
            
            for i in range(0, len(enriched_df), batch_size):
                batch_df = enriched_df.iloc[i:i+batch_size]
                
                # Convert DataFrame to list of dicts
                records = batch_df.to_dict('records')
                
                # Use bulk_insert from sql.dml
                with self.engine.connect() as conn:
                    for record in records:
                        # Build insert SQL
                        columns = list(record.keys())
                        placeholders = [f":{col}" for col in columns]
                        
                        insert_sql = f"""
                        INSERT INTO {full_table_name} 
                        ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                        """
                        
                        conn.execute(text(insert_sql), record)
                    
                    conn.commit()
                
                rows_loaded += len(records)
                
                if rows_loaded % (batch_size * 5) == 0:
                    logger.info(f"  Loaded {rows_loaded:,} / {total_rows:,} rows...")
            
            logger.info(f"✅ Loaded {rows_loaded:,} rows successfully")
            
            # 5. Log lineage
            logger.info(f"📊 Logging data lineage...")
            self.lineage_tracker.log_lineage(
                process_log_id=process_log_id,
                source_schema='source',
                source_table=csv_full_path.stem,
                target_schema=self.bronze_schema,
                target_table=table_name,
                transformation_logic='CSV ingestion with bronze metadata enrichment',
                record_count=rows_loaded
            )
            
            # 6. Record performance metrics
            if self.perf_monitor:
                duration = (datetime.now() - start_time).total_seconds()
                self.perf_monitor.record_metric(
                    process_log_id=process_log_id,
                    metric_name='rows_per_second',
                    metric_value=rows_loaded / duration if duration > 0 else 0,
                    metric_unit='rows/sec'
                )
            
            # 7. End process logging
            duration_seconds = (datetime.now() - start_time).total_seconds()
            self.process_logger.end_process(
                log_id=process_log_id,
                status='SUCCESS',
                rows_processed=rows_loaded,
                rows_inserted=rows_loaded
            )
            
            logger.info(f"⏱️  Duration: {duration_seconds:.2f} seconds")
            logger.info(f"✅ Bronze ingestion completed successfully")
            
            return {
                'rows_loaded': rows_loaded,
                'batch_id': batch_id,
                'process_log_id': process_log_id,
                'duration_seconds': duration_seconds,
                'table_name': full_table_name
            }
            
        except Exception as e:
            # Log error
            error_msg = f"Bronze ingestion failed: {e}"
            logger.error(error_msg, exc_info=True)
            
            self.error_logger.log_exception(
                error=e,
                process_log_id=process_log_id,
                recovery_suggestion="Check CSV file format and database connection"
            )
            
            # End process with failure
            self.process_logger.end_process(
                log_id=process_log_id,
                status='FAILED',
                error_message=str(e)
            )
            
            raise BronzeManagerError(error_msg)
    
    def load_all_crm_data(self) -> Dict[str, Any]:
        """
        Load all CRM CSV files into bronze schema.
        
        Loads:
            - cust_info.csv → bronze.crm_customers_raw
            - prd_info.csv → bronze.crm_products_raw
            - sales_details.csv → bronze.crm_sales_raw
            
        Returns:
            Dictionary with results for each table
        """
        logger.info("\n" + "="*70)
        logger.info("🔵 LOADING ALL CRM DATA")
        logger.info("="*70)
        
        crm_files = [
            ('datasets/source_crm/cust_info.csv', 'crm_customers_raw'),
            ('datasets/source_crm/prd_info.csv', 'crm_products_raw'),
            ('datasets/source_crm/sales_details.csv', 'crm_sales_raw')
        ]
        
        results = {}
        
        for csv_path, table_name in crm_files:
            try:
                result = self.load_csv_to_bronze(
                    csv_path=csv_path,
                    table_name=table_name,
                    source_system='CRM',
                    create_table=True
                )
                results[table_name] = result
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}")
                results[table_name] = {'error': str(e)}
        
        return results
    
    def load_all_erp_data(self) -> Dict[str, Any]:
        """
        Load all ERP CSV files into bronze schema.
        
        Loads:
            - CUST_AZ12.csv → bronze.erp_customers_raw
            - LOC_A101.csv → bronze.erp_locations_raw
            - PX_CAT_G1V2.csv → bronze.erp_product_categories_raw
            
        Returns:
            Dictionary with results for each table
        """
        logger.info("\n" + "="*70)
        logger.info("🔵 LOADING ALL ERP DATA")
        logger.info("="*70)
        
        erp_files = [
            ('datasets/source_erp/CUST_AZ12.csv', 'erp_customers_raw'),
            ('datasets/source_erp/LOC_A101.csv', 'erp_locations_raw'),
            ('datasets/source_erp/PX_CAT_G1V2.csv', 'erp_product_categories_raw')
        ]
        
        results = {}
        
        for csv_path, table_name in erp_files:
            try:
                result = self.load_csv_to_bronze(
                    csv_path=csv_path,
                    table_name=table_name,
                    source_system='ERP',
                    create_table=True
                )
                results[table_name] = result
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}")
                results[table_name] = {'error': str(e)}
        
        return results
    
    def load_all_data(self) -> Dict[str, Any]:
        """
        Load all CRM and ERP data into bronze schema.
        
        Returns:
            Dictionary with results for all tables
        """
        logger.info("\n" + "="*70)
        logger.info("🔵 LOADING ALL BRONZE DATA (CRM + ERP)")
        logger.info("="*70)
        
        results = {}
        
        # Load CRM data
        crm_results = self.load_all_crm_data()
        results.update(crm_results)
        
        # Load ERP data
        erp_results = self.load_all_erp_data()
        results.update(erp_results)
        
        # Summary
        success_count = sum(1 for r in results.values() if 'error' not in r)
        total_count = len(results)
        
        logger.info("\n" + "="*70)
        logger.info(f"✅ BRONZE LOAD COMPLETE: {success_count}/{total_count} tables loaded")
        logger.info("="*70)
        
        return results
    
    def close(self):
        """Close database connections and cleanup resources."""
        if self.engine:
            self.engine.dispose()
            logger.info("🔌 Database connections closed")


def main():
    """
    CLI entry point for bronze layer operations.
    
    Usage:
        python -m medallion.bronze
    """
    from core.logger import setup_logging

    # Setup logging
    setup_logging(
        log_level='INFO',
        log_file='bronze_ingestion.log',
        log_dir='logs'
    )
    
    try:
        # Initialize manager
        manager = BronzeManager(monitor_performance=True)
        
        # Load all data
        results = manager.load_all_data()
        
        # Print summary
        print("\n" + "="*70)
        print("BRONZE INGESTION SUMMARY")
        print("="*70)
        
        for table_name, result in results.items():
            if 'error' in result:
                print(f"❌ {table_name}: {result['error']}")
            else:
                print(f"✅ {table_name}: {result['rows_loaded']:,} rows in {result['duration_seconds']:.2f}s")
        
        # Cleanup
        manager.close()
        
        return 0
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())