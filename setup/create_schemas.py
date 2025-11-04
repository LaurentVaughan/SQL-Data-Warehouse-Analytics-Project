"""
==================================================
Schema creation module for medallion architecture.
==================================================

Provides SQLAlchemy-based schema creation for the medallion architecture
(bronze/silver/gold/logs layers). Replaces the original SQL script with
improved error handling and programmatic control.

The medallion architecture organizes data into layers:
    - Bronze: Raw, unprocessed data from source systems
    - Silver: Cleansed, validated, and conformed data
    - Gold: Business-ready aggregated and enriched data
    - Logs: Audit trails and process logging

Key Features:
    - SQLAlchemy-based schema creation with type safety
    - Better error handling and logging
    - Programmatic control over schema operations
    - Idempotent execution (safe to run multiple times)
    - Integration with Python ETL pipelines
    - Uses sql.query_builder for metadata queries

Prerequisites:
    - Target warehouse database exists
    - Connection with schema creation privileges
    - SQLAlchemy and psycopg2 dependencies

Example:
    >>> from setup.create_schemas import SchemaCreator
    >>> 
    >>> creator = SchemaCreator(
    ...     host='localhost',
    ...     user='postgres',
    ...     password='password',
    ...     database='warehouse'
    ... )
    >>> 
    >>> # Create all medallion schemas
    >>> results = creator.create_all_schemas()
    >>> 
    >>> # Or create individual schemas
    >>> creator.create_bronze_schema()
    >>> creator.create_silver_schema()
    >>> creator.create_gold_schema()
    >>> creator.create_logs_schema()
"""

import logging
from typing import Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import Engine, MetaData, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema

from sql.query_builder import check_schema_exists_sql, get_schema_info_sql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaCreationError(Exception):
    """Exception raised for schema creation operation errors.
    
    Raised when schema creation, verification, or metadata operations fail.
    """
    pass


class SchemaCreator:
    """SQLAlchemy-based schema creation for medallion architecture.
    
    Creates and manages the four core schemas of the medallion architecture
    with proper error handling and idempotent execution. Uses SQLAlchemy
    for type-safe schema operations.
    
    Attributes:
        host: PostgreSQL server hostname
        port: PostgreSQL server port
        user: Database username with schema creation privileges
        password: Database password
        database: Target warehouse database name
        SCHEMAS: Dict mapping schema names to descriptions
    
    Schema Definitions:
        - bronze: Raw data layer for unprocessed source data
        - silver: Cleansed data layer for validated, conformed data
        - gold: Business data layer for aggregated analytics
        - logs: Logging infrastructure for audit trails
    
    Example:
        >>> creator = SchemaCreator(
        ...     host='localhost',
        ...     user='postgres',
        ...     password='pwd',
        ...     database='warehouse'
        ... )
        >>> results = creator.create_all_schemas()
        >>> if all(results.values()):
        ...     print("All schemas created successfully")
    """
    
    # Schema definitions matching the medallion architecture
    SCHEMAS = {
        'bronze': 'Raw data layer - Unprocessed source data from CRM/ERP systems',
        'silver': 'Cleansed data layer - Validated, deduplicated, and conformed data',
        'gold': 'Business data layer - Aggregated, enriched data for analytics',
        'logs': 'Logging infrastructure - Medallion architecture audit trails'
    }
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        password: str = '',
        database: str = 'sql_retail_analytics_warehouse'
    ):
        """Initialize the schema creator with connection parameters.
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port number
            user: Database user with schema creation privileges
            password: Database password
            database: Target warehouse database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        self._engine: Optional[Engine] = None
        self._metadata: Optional[MetaData] = None
        
    def _get_engine(self) -> Engine:
        """Get SQLAlchemy engine connected to target database."""
        if self._engine is None:
            connection_string = (
                f"postgresql://{self.user}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            self._engine = create_engine(connection_string, echo=False)
        return self._engine
    
    def _get_metadata(self) -> MetaData:
        """Get SQLAlchemy metadata object."""
        if self._metadata is None:
            self._metadata = MetaData()
        return self._metadata
    
    def check_schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema exists.
        
        Args:
            schema_name: Name of the schema to check
            
        Returns:
            True if schema exists, False otherwise
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                # Use SQL from query_builder module
                sql = check_schema_exists_sql(schema_name)
                result = conn.execute(text(sql))
                return result.fetchone() is not None
        except SQLAlchemyError as e:
            logger.error(f"Error checking schema existence: {e}")
            raise SchemaCreationError(f"Failed to check schema existence: {e}")
    
    def create_schema(self, schema_name: str, description: str, if_not_exists: bool = True) -> bool:
        """
        Create a single schema with description.
        
        Args:
            schema_name: Name of the schema to create
            description: Description comment for the schema
            if_not_exists: If True, use IF NOT EXISTS logic
            
        Returns:
            True if schema was created, False if it already existed
        """
        try:
            engine = self._get_engine()
            
            # Check if schema already exists
            if if_not_exists and self.check_schema_exists(schema_name):
                logger.info(f"Schema '{schema_name}' already exists, skipping creation")
                return False
            
            with engine.connect() as conn:
                # Create schema
                conn.execute(CreateSchema(schema_name, if_not_exists=if_not_exists))
                
                # Add comment
                conn.execute(
                    text(f"COMMENT ON SCHEMA {schema_name} IS :description"),
                    {"description": description}
                )
                
                conn.commit()
                
            logger.info(f"Created schema '{schema_name}': {description}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error creating schema '{schema_name}': {e}")
            raise SchemaCreationError(f"Failed to create schema '{schema_name}': {e}")
    
    def create_bronze_schema(self) -> bool:
        """Create the bronze schema for raw data ingestion."""
        return self.create_schema('bronze', self.SCHEMAS['bronze'])
    
    def create_silver_schema(self) -> bool:
        """Create the silver schema for cleansed data."""
        return self.create_schema('silver', self.SCHEMAS['silver'])
    
    def create_gold_schema(self) -> bool:
        """Create the gold schema for analytics data."""
        return self.create_schema('gold', self.SCHEMAS['gold'])
    
    def create_logs_schema(self) -> bool:
        """Create the logs schema for audit trails."""
        return self.create_schema('logs', self.SCHEMAS['logs'])
    
    def create_all_schemas(self) -> Dict[str, bool]:
        """
        Create all medallion architecture schemas.
        
        Returns:
            Dictionary mapping schema names to creation status (True=created, False=existed)
        """
        logger.info("🚀 Creating medallion architecture schemas...")
        
        results = {}
        
        try:
            # Create schemas in logical order
            results['logs'] = self.create_logs_schema()      # First - needed for logging
            results['bronze'] = self.create_bronze_schema()  # Second - raw data layer
            results['silver'] = self.create_silver_schema()  # Third - cleansed data
            results['gold'] = self.create_gold_schema()      # Fourth - analytics layer
            
            created_count = sum(results.values())
            total_count = len(results)
            existing_count = total_count - created_count
            
            if created_count > 0:
                logger.info(f"✅ Created {created_count} new schemas")
            if existing_count > 0:
                logger.info(f"ℹ️  {existing_count} schemas already existed")
                
            logger.info("✅ Medallion architecture schemas setup completed successfully")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to create schemas: {e}")
            raise
    
    def verify_all_schemas(self) -> Dict[str, bool]:
        """
        Verify that all required schemas exist.
        
        Returns:
            Dictionary mapping schema names to existence status
        """
        results = {}
        
        for schema_name in self.SCHEMAS.keys():
            results[schema_name] = self.check_schema_exists(schema_name)
        
        missing_schemas = [name for name, exists in results.items() if not exists]
        
        if missing_schemas:
            logger.warning(f"Missing schemas: {missing_schemas}")
        else:
            logger.info("✅ All required schemas exist")
        
        return results
    
    def get_schema_info(self) -> List[Dict[str, str]]:
        """
        Get information about all schemas in the database.
        
        Returns:
            List of dictionaries containing schema information
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                # Use SQL from query_builder module
                sql = get_schema_info_sql(list(self.SCHEMAS.keys()))
                result = conn.execute(text(sql))
                
                schemas = []
                for row in result:
                    schemas.append({
                        'schema_name': row.schema_name,
                        'schema_owner': row.schema_owner,
                        'description': row.description or 'No description'
                    })
                
                return schemas
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting schema information: {e}")
            raise SchemaCreationError(f"Failed to get schema information: {e}")
    
    def close_connections(self) -> None:
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


def main():
    """
    Main function for command-line usage.
    Equivalent to running the original create_schemas.sql script.
    """
    import os

    # Get database credentials from environment or use defaults
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'sql_retail_analytics_warehouse')
    }
    
    try:
        creator = SchemaCreator(**db_config)
        
        # Create all schemas
        results = creator.create_all_schemas()
        
        # Verify creation
        verification = creator.verify_all_schemas()
        
        # Get schema information
        schema_info = creator.get_schema_info()
        
        print("\n✅ Schema creation completed successfully!")
        
        print("\nSchema Creation Results:")
        for schema_name, was_created in results.items():
            status = "Created" if was_created else "Already existed"
            print(f"  {schema_name}: {status}")
        
        print("\nSchema Details:")
        for schema in schema_info:
            print(f"  {schema['schema_name']}: {schema['description']}")
        
        print(f"\nNext steps:")
        print(f"1. Create logging infrastructure: python -m setup.create_logs")
        print(f"2. Seed configuration: python -m setup.seed.seed_configuration")
        print(f"3. Continue with bronze layer setup")
        
    except SchemaCreationError as e:
        logger.error(f"Schema creation failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    finally:
        if 'creator' in locals():
            creator.close_connections()
    
    return 0


if __name__ == '__main__':
    exit(main())