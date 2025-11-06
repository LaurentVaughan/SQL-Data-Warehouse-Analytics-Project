"""
=======================================================================
Data Definition Language (DDL) utilities for schema and table creation.
=======================================================================

Provides reusable functions for generating PostgreSQL DDL statements with
best practices for medallion architecture, performance optimization, and
data governance.

Key Features:
    - Schema creation with authorization and comments
    - Table creation with medallion metadata columns
    - Index creation with performance optimizations
    - Constraint management
    - Partitioning support
    - Database creation and management utilities

Functions:
    create_schema: Generate CREATE SCHEMA statements
    create_table: Generate CREATE TABLE with medallion patterns
    create_index: Generate CREATE INDEX with optimizations
    create_constraint: Generate constraint definitions
    create_medallion_table_template: Full medallion table with all metadata
    create_database_sql: Generate CREATE DATABASE statement
    drop_database_sql: Generate DROP DATABASE statement  
    terminate_connections_sql: Terminate database connections

Example:
    >>> from sql.ddl import create_table, create_medallion_table_template
    >>> 
    >>> # Simple table creation
    >>> table_sql = create_table(
    ...     schema='bronze',
    ...     table='customer_data',
    ...     columns=[
    ...         {'name': 'customer_id', 'type': 'INTEGER', 'constraints': ['PRIMARY KEY']},
    ...         {'name': 'customer_name', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']}
    ...     ],
    ...     medallion_metadata=True,
    ...     comment='Raw customer data from CRM'
    ... )
    >>> 
    >>> # Full medallion table with partitioning
    >>> medallion_sql = create_medallion_table_template(
    ...     schema='bronze',
    ...     table='sales_transactions',
    ...     business_columns=[
    ...         {'name': 'transaction_id', 'type': 'BIGINT'},
    ...         {'name': 'amount', 'type': 'DECIMAL(10,2)'}
    ...     ],
    ...     partition_by='created_at',
    ...     comment='Sales transaction data'
    ... )
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union


def create_schema(
    schema_name: str,
    if_not_exists: bool = True,
    comment: Optional[str] = None,
    authorization: Optional[str] = None
) -> str:
    """Generate CREATE SCHEMA statement.
    
    Args:
        schema_name: Name of the schema to create
        if_not_exists: If True, add IF NOT EXISTS clause
        comment: Optional descriptive comment for the schema
        authorization: Optional schema owner username
        
    Returns:
        SQL CREATE SCHEMA statement with optional comment
        
    Example:
        >>> sql = create_schema('bronze', comment='Raw data layer')
        >>> print(sql)
        CREATE SCHEMA IF NOT EXISTS "bronze";
        COMMENT ON SCHEMA "bronze" IS 'Raw data layer';
    """
    sql_parts = ["CREATE SCHEMA"]
    
    if if_not_exists:
        sql_parts.append("IF NOT EXISTS")
    
    sql_parts.append(f'"{schema_name}"')
    
    if authorization:
        sql_parts.append(f"AUTHORIZATION {authorization}")
    
    sql = " ".join(sql_parts) + ";"
    
    if comment:
        sql += f"\nCOMMENT ON SCHEMA \"{schema_name}\" IS '{comment}';"
    
    return sql


def create_table(
    schema: str,
    table: str,
    columns: List[Dict[str, Any]],
    medallion_metadata: bool = True,
    partitioning: Optional[Dict[str, Any]] = None,
    indexes: Optional[List[Dict[str, Any]]] = None,
    constraints: Optional[List[str]] = None,
    if_not_exists: bool = True,
    comment: Optional[str] = None
) -> str:
    """Generate CREATE TABLE statement with medallion architecture patterns.
    
    Creates table with optional medallion metadata columns (created_at,
    updated_at, source_system, etc.) and supports partitioning, indexes,
    and constraints.
    
    Args:
        schema: Schema name for the table
        table: Table name
        columns: List of column dicts with keys: name, type, constraints, default
        medallion_metadata: If True, add standard medallion metadata columns
        partitioning: Optional dict with partition configuration
        indexes: Optional list of index definitions
        constraints: Optional list of table-level constraints
        if_not_exists: If True, add IF NOT EXISTS clause
        comment: Optional table description
        
    Returns:
        Complete SQL CREATE TABLE statement with indexes and constraints
        
    Example:
        >>> sql = create_table(
        ...     schema='silver',
        ...     table='customers',
        ...     columns=[
        ...         {'name': 'id', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY']},
        ...         {'name': 'name', 'type': 'VARCHAR(255)', 'constraints': ['NOT NULL']},
        ...         {'name': 'email', 'type': 'VARCHAR(255)', 'constraints': ['UNIQUE']}
        ...     ],
        ...     medallion_metadata=True,
        ...     comment='Cleansed customer master data'
        ... )
    """
    # Start building the CREATE TABLE statement
    sql_parts = ["CREATE TABLE"]
    
    if if_not_exists:
        sql_parts.append("IF NOT EXISTS")
    
    sql_parts.append(f'"{schema}"."{table}" (')
    
    # Build column definitions
    column_defs = []
    
    # Add user-defined columns
    for col in columns:
        col_def = f'    "{col["name"]}" {col["type"]}'
        
        # Add constraints
        if "constraints" in col:
            for constraint in col["constraints"]:
                col_def += f" {constraint}"
        
        # Add default values
        if "default" in col:
            col_def += f" DEFAULT {col['default']}"
        
        # Add column comment
        if "comment" in col:
            # Note: Column comments are added separately in PostgreSQL
            pass
            
        column_defs.append(col_def)
    
    # Add medallion metadata columns if requested
    if medallion_metadata:
        metadata_columns = [
            '    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL',
            '    "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL',
            '    "created_by" VARCHAR(50) DEFAULT \'system\' NOT NULL',
            '    "updated_by" VARCHAR(50) DEFAULT \'system\' NOT NULL',
            '    "source_system" VARCHAR(50)',
            '    "batch_id" VARCHAR(100)',
            '    "is_deleted" BOOLEAN DEFAULT FALSE NOT NULL',
            '    "row_hash" VARCHAR(64)'
        ]
        column_defs.extend(metadata_columns)
    
    # Add table constraints if specified
    if constraints:
        for constraint in constraints:
            column_defs.append(f"    {constraint}")
    
    # Complete the CREATE TABLE statement
    sql = " ".join(sql_parts) + "\n" + ",\n".join(column_defs) + "\n)"
    
    # Add partitioning if specified
    if partitioning:
        if partitioning["type"] == "RANGE":
            sql += f"\nPARTITION BY RANGE ({partitioning['column']})"
        elif partitioning["type"] == "HASH":
            sql += f"\nPARTITION BY HASH ({partitioning['column']})"
        elif partitioning["type"] == "LIST":
            sql += f"\nPARTITION BY LIST ({partitioning['column']})"
    
    sql += ";"
    
    # Add table comment
    if comment:
        sql += f"\nCOMMENT ON TABLE \"{schema}\".\"{table}\" IS '{comment}';"
    
    # Add column comments
    for col in columns:
        if "comment" in col:
            sql += f"\nCOMMENT ON COLUMN \"{schema}\".\"{table}\".\"{col['name']}\" IS '{col['comment']}';"
    
    # Add indexes if specified
    if indexes:
        for index in indexes:
            sql += "\n" + create_index(
                schema=schema,
                table=table,
                index_name=index.get("name"),
                columns=index["columns"],
                index_type=index.get("type", "BTREE"),
                unique=index.get("unique", False),
                where_clause=index.get("where")
            )
    
    return sql


def create_index(
    schema: str,
    table: str,
    columns: Union[str, List[str]],
    index_name: Optional[str] = None,
    index_type: str = "BTREE",
    unique: bool = False,
    where_clause: Optional[str] = None,
    if_not_exists: bool = True
) -> str:
    """
    Generate CREATE INDEX statement.
    
    Args:
        schema: Schema name
        table: Table name
        columns: Column name(s) for the index
        index_name: Optional index name (auto-generated if not provided)
        index_type: Index type (BTREE, HASH, GIN, GIST)
        unique: Create unique index
        where_clause: Partial index WHERE clause
        if_not_exists: Add IF NOT EXISTS clause
        
    Returns:
        SQL CREATE INDEX statement
    """
    # Handle columns parameter
    if isinstance(columns, str):
        columns = [columns]
    
    # Generate index name if not provided
    if not index_name:
        column_suffix = "_".join(columns)
        unique_suffix = "_unique" if unique else ""
        index_name = f"idx_{table}_{column_suffix}{unique_suffix}"
    
    # Build the CREATE INDEX statement
    sql_parts = ["CREATE"]
    
    if unique:
        sql_parts.append("UNIQUE")
    
    sql_parts.append("INDEX")
    
    if if_not_exists:
        sql_parts.append("IF NOT EXISTS")
    
    sql_parts.append(f'"{index_name}"')
    sql_parts.append("ON")
    sql_parts.append(f'"{schema}"."{table}"')
    
    if index_type != "BTREE":
        sql_parts.append(f"USING {index_type}")
    
    # Add column list
    column_list = ", ".join([f'"{col}"' for col in columns])
    sql_parts.append(f"({column_list})")
    
    # Add WHERE clause for partial indexes
    if where_clause:
        sql_parts.append(f"WHERE {where_clause}")
    
    return " ".join(sql_parts) + ";"


def create_constraint(
    schema: str,
    table: str,
    constraint_name: str,
    constraint_type: str,
    columns: Optional[List[str]] = None,
    reference_table: Optional[str] = None,
    reference_columns: Optional[List[str]] = None,
    check_condition: Optional[str] = None
) -> str:
    """
    Generate ALTER TABLE ADD CONSTRAINT statement.
    
    Args:
        schema: Schema name
        table: Table name
        constraint_name: Name of the constraint
        constraint_type: Type (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
        columns: Columns involved in the constraint
        reference_table: Referenced table for foreign keys
        reference_columns: Referenced columns for foreign keys
        check_condition: Condition for CHECK constraints
        
    Returns:
        SQL ALTER TABLE ADD CONSTRAINT statement
    """
    sql = f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{constraint_name}"'
    
    if constraint_type == "PRIMARY KEY":
        column_list = ", ".join([f'"{col}"' for col in columns])
        sql += f" PRIMARY KEY ({column_list})"
    
    elif constraint_type == "FOREIGN KEY":
        column_list = ", ".join([f'"{col}"' for col in columns])
        ref_column_list = ", ".join([f'"{col}"' for col in reference_columns])
        sql += f" FOREIGN KEY ({column_list}) REFERENCES \"{schema}\".\"{reference_table}\" ({ref_column_list})"
    
    elif constraint_type == "UNIQUE":
        column_list = ", ".join([f'"{col}"' for col in columns])
        sql += f" UNIQUE ({column_list})"
    
    elif constraint_type == "CHECK":
        sql += f" CHECK ({check_condition})"
    
    return sql + ";"


def alter_table_add_column(
    schema: str,
    table: str,
    column_name: str,
    column_type: str,
    constraints: Optional[List[str]] = None,
    default_value: Optional[str] = None,
    if_not_exists: bool = True
) -> str:
    """
    Generate ALTER TABLE ADD COLUMN statement.
    
    Args:
        schema: Schema name
        table: Table name
        column_name: Name of the column to add
        column_type: Data type of the column
        constraints: List of constraints
        default_value: Default value for the column
        if_not_exists: Add IF NOT EXISTS clause
        
    Returns:
        SQL ALTER TABLE ADD COLUMN statement
    """
    sql = f'ALTER TABLE "{schema}"."{table}" ADD COLUMN'
    
    if if_not_exists:
        sql += " IF NOT EXISTS"
    
    sql += f' "{column_name}" {column_type}'
    
    if default_value:
        sql += f" DEFAULT {default_value}"
    
    if constraints:
        for constraint in constraints:
            sql += f" {constraint}"
    
    return sql + ";"


def drop_schema(
    schema_name: str,
    if_exists: bool = True,
    cascade: bool = True
) -> str:
    """Generate DROP SCHEMA statement.
    
    Args:
        schema_name: Name of the schema to drop
        if_exists: If True, add IF EXISTS clause
        cascade: If True, drop all contained objects automatically
        
    Returns:
        SQL DROP SCHEMA statement
        
    Example:
        >>> sql = drop_schema('bronze', cascade=True)
        >>> print(sql)
        DROP SCHEMA IF EXISTS "bronze" CASCADE;
    """
    sql = "DROP SCHEMA"
    
    if if_exists:
        sql += " IF EXISTS"
    
    sql += f' "{schema_name}"'
    
    if cascade:
        sql += " CASCADE"
    
    return sql + ";"


def drop_table(
    schema: str,
    table: str,
    if_exists: bool = True,
    cascade: bool = False
) -> str:
    """
    Generate DROP TABLE statement.
    
    Args:
        schema: Schema name
        table: Table name
        if_exists: Add IF EXISTS clause
        cascade: Add CASCADE option
        
    Returns:
        SQL DROP TABLE statement
    """
    sql = "DROP TABLE"
    
    if if_exists:
        sql += " IF EXISTS"
    
    sql += f' "{schema}"."{table}"'
    
    if cascade:
        sql += " CASCADE"
    
    return sql + ";"


def create_medallion_table_template(
    schema: str,
    table: str,
    business_columns: List[Dict[str, Any]],
    partition_by: Optional[str] = None,
    comment: Optional[str] = None
) -> str:
    """
    Create a standardized medallion architecture table with all required metadata.
    
    Args:
        schema: Medallion layer (bronze, silver, gold)
        table: Business table name
        business_columns: Business-specific column definitions
        partition_by: Column to partition by (usually created_at)
        comment: Table description
        
    Returns:
        Complete CREATE TABLE statement with indexes and constraints
    """
    # Standard indexes for medallion tables
    standard_indexes = [
        {
            "columns": ["created_at"],
            "name": f"idx_{table}_created_at"
        },
        {
            "columns": ["batch_id"],
            "name": f"idx_{table}_batch_id"
        },
        {
            "columns": ["source_system"],
            "name": f"idx_{table}_source_system"
        },
        {
            "columns": ["is_deleted"],
            "name": f"idx_{table}_is_deleted",
            "where": "is_deleted = FALSE"
        }
    ]
    
    # Add partitioning configuration if specified
    partitioning = None
    if partition_by:
        partitioning = {
            "type": "RANGE",
            "column": partition_by
        }
    
    # Generate the complete table definition
    return create_table(
        schema=schema,
        table=table,
        columns=business_columns,
        medallion_metadata=True,
        partitioning=partitioning,
        indexes=standard_indexes,
        comment=comment or f"{schema.title()} layer table for {table}"
    )


def create_database_sql(
    database_name: str,
    template: str = 'template0',
    encoding: str = 'UTF8',
    lc_collate: str = 'en_GB.UTF-8',
    lc_ctype: str = 'en_GB.UTF-8',
    owner: Optional[str] = None
) -> str:
    """
    Generate CREATE DATABASE statement.
    
    Note: This returns the SQL string. Database creation requires special
    connection handling (AUTOCOMMIT isolation) which must be done separately.
    
    Args:
        database_name: Name of the database to create
        template: Template database to use
        encoding: Character encoding
        lc_collate: Collation order
        lc_ctype: Character classification
        owner: Optional database owner
        
    Returns:
        SQL CREATE DATABASE statement
    """
    sql = f"""CREATE DATABASE "{database_name}"
    WITH TEMPLATE = '{template}'
         ENCODING = '{encoding}'
         LC_COLLATE = '{lc_collate}'
         LC_CTYPE = '{lc_ctype}'"""
    
    if owner:
        sql += f"\n         OWNER = {owner}"
    
    return sql + ";"


def drop_database_sql(
    database_name: str,
    if_exists: bool = True,
    force: bool = True
) -> str:
    """
    Generate DROP DATABASE statement.
    
    Note: This returns the SQL string. Database dropping requires special
    connection handling (AUTOCOMMIT isolation) which must be done separately.
    
    Args:
        database_name: Name of the database to drop
        if_exists: Add IF EXISTS clause
        force: Add WITH (FORCE) clause (PostgreSQL 13+)
        
    Returns:
        SQL DROP DATABASE statement
    """
    sql_parts = ["DROP DATABASE"]
    
    if if_exists:
        sql_parts.append("IF EXISTS")
    
    sql_parts.append(f'"{database_name}"')
    
    if force:
        sql_parts.append("WITH (FORCE)")
    
    return " ".join(sql_parts) + ";"


def terminate_connections_sql(database_name: str) -> str:
    """
    Generate SQL to terminate all connections to a database.
    
    Args:
        database_name: Name of the database
        
    Returns:
        SQL to terminate connections
    """
    return f"""
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '{database_name}'
  AND pid <> pg_backend_pid();
"""