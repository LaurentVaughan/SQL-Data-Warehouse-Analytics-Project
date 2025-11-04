"""
===========================================
Data Manipulation Language (DML) Utilities.
===========================================

This module provides reusable functions for creating SQL DML statements.
Focuses on INSERT, UPDATE, DELETE operations with medallion architecture patterns.

Functions:
- bulk_insert: Generate efficient bulk INSERT statements
- upsert: Generate INSERT ... ON CONFLICT (upsert) statements
- soft_delete: Generate UPDATE statements for soft deletes
- merge_statement: Generate MERGE-like operations using CTEs
- batch_update: Generate efficient batch UPDATE statements
- incremental_load: Generate incremental data loading statements

Usage:
    from sql.dml import bulk_insert, upsert, soft_delete
    
    # Bulk insert with conflict resolution
    insert_sql = bulk_insert(
        schema='bronze',
        table='customer_data',
        columns=['customer_id', 'customer_name', 'email'],
        on_conflict='DO NOTHING'
    )
    
    # Upsert operation
    upsert_sql = upsert(
        schema='silver',
        table='customers',
        columns=['customer_id', 'customer_name', 'email'],
        key_columns=['customer_id'],
        update_columns=['customer_name', 'email']
    )
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union


def bulk_insert(
    schema: str,
    table: str,
    columns: List[str],
    on_conflict: Optional[str] = None,
    returning: Optional[List[str]] = None,
    include_metadata: bool = True
) -> str:
    """
    Generate bulk INSERT statement with optional conflict resolution.
    
    Args:
        schema: Schema name
        table: Table name
        columns: List of column names to insert
        on_conflict: Conflict resolution (DO NOTHING, DO UPDATE SET ...)
        returning: Columns to return after insert
        include_metadata: Add medallion metadata columns
        
    Returns:
        SQL INSERT statement template
    """
    # Add medallion metadata columns if requested
    if include_metadata:
        metadata_columns = [
            'created_at', 'updated_at', 'created_by', 'updated_by',
            'source_system', 'batch_id'
        ]
        all_columns = columns + [col for col in metadata_columns if col not in columns]
    else:
        all_columns = columns
    
    # Build the INSERT statement
    column_list = ", ".join([f'"{col}"' for col in all_columns])
    placeholder_list = ", ".join([f"$${col}$$" for col in all_columns])
    
    sql = f'''INSERT INTO "{schema}"."{table}" (
    {column_list}
) VALUES (
    {placeholder_list}
)'''
    
    # Add conflict resolution
    if on_conflict:
        sql += f"\nON CONFLICT {on_conflict}"
    
    # Add RETURNING clause
    if returning:
        return_list = ", ".join([f'"{col}"' for col in returning])
        sql += f"\nRETURNING {return_list}"
    
    return sql + ";"


def upsert(
    schema: str,
    table: str,
    columns: List[str],
    key_columns: List[str],
    update_columns: Optional[List[str]] = None,
    where_condition: Optional[str] = None,
    include_metadata: bool = True
) -> str:
    """
    Generate INSERT ... ON CONFLICT DO UPDATE (upsert) statement.
    
    Args:
        schema: Schema name
        table: Table name
        columns: All columns to insert
        key_columns: Columns that define uniqueness
        update_columns: Columns to update on conflict (defaults to all non-key columns)
        where_condition: Additional WHERE condition for the UPDATE
        include_metadata: Add medallion metadata handling
        
    Returns:
        SQL UPSERT statement template
    """
    # Determine which columns to update on conflict
    if update_columns is None:
        update_columns = [col for col in columns if col not in key_columns]
    
    # Add medallion metadata columns if requested
    if include_metadata:
        metadata_columns = [
            'created_at', 'updated_at', 'created_by', 'updated_by',
            'source_system', 'batch_id'
        ]
        all_columns = columns + [col for col in metadata_columns if col not in columns]
        
        # For updates, we want to update the updated_at and updated_by fields
        if 'updated_at' not in update_columns:
            update_columns.append('updated_at')
        if 'updated_by' not in update_columns:
            update_columns.append('updated_by')
    else:
        all_columns = columns
    
    # Build the INSERT part
    column_list = ", ".join([f'"{col}"' for col in all_columns])
    placeholder_list = ", ".join([f"$${col}$$" for col in all_columns])
    
    sql = f'''INSERT INTO "{schema}"."{table}" (
    {column_list}
) VALUES (
    {placeholder_list}
)
ON CONFLICT ({", ".join([f'"{col}"' for col in key_columns])}) DO UPDATE SET'''
    
    # Build the UPDATE part
    update_clauses = []
    for col in update_columns:
        if include_metadata and col == 'updated_at':
            update_clauses.append(f'    "{col}" = CURRENT_TIMESTAMP')
        elif include_metadata and col == 'updated_by':
            update_clauses.append(f'    "{col}" = $$updated_by$$')
        else:
            update_clauses.append(f'    "{col}" = EXCLUDED."{col}"')
    
    sql += "\n" + ",\n".join(update_clauses)
    
    # Add WHERE condition if specified
    if where_condition:
        sql += f"\nWHERE {where_condition}"
    
    return sql + ";"


def soft_delete(
    schema: str,
    table: str,
    where_condition: str,
    deleted_by: str = 'system',
    include_metadata: bool = True
) -> str:
    """
    Generate soft delete UPDATE statement.
    
    Args:
        schema: Schema name
        table: Table name
        where_condition: WHERE clause to identify records to delete
        deleted_by: User performing the soft delete
        include_metadata: Update medallion metadata columns
        
    Returns:
        SQL UPDATE statement for soft delete
    """
    sql = f'UPDATE "{schema}"."{table}" SET'
    
    update_clauses = ['    "is_deleted" = TRUE']
    
    if include_metadata:
        update_clauses.extend([
            '    "updated_at" = CURRENT_TIMESTAMP',
            f'    "updated_by" = \'{deleted_by}\''
        ])
    
    sql += "\n" + ",\n".join(update_clauses)
    sql += f"\nWHERE {where_condition}"
    
    return sql + ";"


def batch_update(
    schema: str,
    table: str,
    update_columns: Dict[str, str],
    where_condition: str,
    include_metadata: bool = True,
    updated_by: str = 'system'
) -> str:
    """
    Generate batch UPDATE statement.
    
    Args:
        schema: Schema name
        table: Table name
        update_columns: Dictionary of column_name: value/expression
        where_condition: WHERE clause to identify records to update
        include_metadata: Update medallion metadata columns
        updated_by: User performing the update
        
    Returns:
        SQL UPDATE statement
    """
    sql = f'UPDATE "{schema}"."{table}" SET'
    
    # Build update clauses
    update_clauses = []
    for column, value in update_columns.items():
        update_clauses.append(f'    "{column}" = {value}')
    
    # Add metadata updates
    if include_metadata:
        update_clauses.extend([
            '    "updated_at" = CURRENT_TIMESTAMP',
            f'    "updated_by" = \'{updated_by}\''
        ])
    
    sql += "\n" + ",\n".join(update_clauses)
    sql += f"\nWHERE {where_condition}"
    
    return sql + ";"


def merge_statement(
    target_schema: str,
    target_table: str,
    source_query: str,
    key_columns: List[str],
    insert_columns: List[str],
    update_columns: Optional[List[str]] = None,
    delete_condition: Optional[str] = None,
    include_metadata: bool = True
) -> str:
    """
    Generate MERGE-like operation using CTEs and conditional logic.
    
    Args:
        target_schema: Target schema name
        target_table: Target table name
        source_query: Source data query (can be a table or complex query)
        key_columns: Columns used for matching
        insert_columns: Columns for INSERT operation
        update_columns: Columns for UPDATE operation
        delete_condition: Condition for DELETE operation
        include_metadata: Handle medallion metadata
        
    Returns:
        SQL MERGE statement using CTEs
    """
    if update_columns is None:
        update_columns = [col for col in insert_columns if col not in key_columns]
    
    # Build the MERGE using CTEs
    key_join = " AND ".join([f't."{col}" = s."{col}"' for col in key_columns])
    
    sql = f'''WITH source_data AS (
    {source_query}
),
merge_action AS (
    SELECT 
        s.*,
        CASE 
            WHEN t.{key_columns[0]} IS NULL THEN 'INSERT'
            WHEN t.{key_columns[0]} IS NOT NULL THEN 'UPDATE'
        END as merge_action
    FROM source_data s
    LEFT JOIN "{target_schema}"."{target_table}" t
        ON {key_join}
),
insert_data AS (
    INSERT INTO "{target_schema}"."{target_table}" (
        {", ".join([f'"{col}"' for col in insert_columns])}
        {', "created_at", "updated_at"' if include_metadata else ''}
    )
    SELECT 
        {", ".join([f'"{col}"' for col in insert_columns])}
        {', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP' if include_metadata else ''}
    FROM merge_action
    WHERE merge_action = 'INSERT'
    RETURNING {key_columns[0]} as inserted_id
),
update_data AS (
    UPDATE "{target_schema}"."{target_table}" t
    SET 
        {", ".join([f'"{col}" = s."{col}"' for col in update_columns])}
        {', "updated_at" = CURRENT_TIMESTAMP' if include_metadata else ''}
    FROM merge_action s
    WHERE 
        {" AND ".join([f't."{col}" = s."{col}"' for col in key_columns])}
        AND s.merge_action = 'UPDATE'
    RETURNING t.{key_columns[0]} as updated_id
)'''
    
    # Add delete operation if specified
    if delete_condition:
        sql += f''',
delete_data AS (
    DELETE FROM "{target_schema}"."{target_table}"
    WHERE {delete_condition}
    RETURNING {key_columns[0]} as deleted_id
)'''
    
    # Final SELECT to show results
    sql += '''
SELECT 
    (SELECT COUNT(*) FROM insert_data) as inserted_count,
    (SELECT COUNT(*) FROM update_data) as updated_count'''
    
    if delete_condition:
        sql += ''',
    (SELECT COUNT(*) FROM delete_data) as deleted_count'''
    
    return sql + ";"


def incremental_load(
    target_schema: str,
    target_table: str,
    source_query: str,
    incremental_column: str,
    last_processed_value: Optional[str] = None,
    batch_size: Optional[int] = None,
    include_metadata: bool = True
) -> str:
    """
    Generate incremental data loading statement.
    
    Args:
        target_schema: Target schema name
        target_table: Target table name
        source_query: Source data query
        incremental_column: Column used for incremental loading (e.g., updated_at)
        last_processed_value: Last processed value for incremental loading
        batch_size: Optional batch size limit
        include_metadata: Add medallion metadata
        
    Returns:
        SQL statement for incremental loading
    """
    # Build the incremental WHERE condition
    where_clauses = []
    
    if last_processed_value:
        where_clauses.append(f'"{incremental_column}" > \'{last_processed_value}\'')
    
    # Add batch size limit
    limit_clause = ""
    if batch_size:
        limit_clause = f"LIMIT {batch_size}"
    
    # Build the complete query
    where_condition = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    sql = f'''WITH incremental_source AS (
    SELECT *
    FROM ({source_query}) src
    WHERE {where_condition}
    ORDER BY "{incremental_column}"
    {limit_clause}
)
INSERT INTO "{target_schema}"."{target_table}"
SELECT 
    *
    {', CURRENT_TIMESTAMP as created_at, CURRENT_TIMESTAMP as updated_at' if include_metadata else ''}
FROM incremental_source'''
    
    return sql + ";"


def generate_copy_statement(
    schema: str,
    table: str,
    file_path: str,
    columns: Optional[List[str]] = None,
    delimiter: str = ',',
    header: bool = True,
    encoding: str = 'UTF8',
    null_string: str = '',
    quote_char: str = '"',
    escape_char: str = '"'
) -> str:
    """
    Generate COPY statement for bulk loading from CSV files.
    
    Args:
        schema: Schema name
        table: Table name
        file_path: Path to the CSV file
        columns: List of columns to load (optional)
        delimiter: Field delimiter
        header: Whether CSV has header row
        encoding: File encoding
        null_string: String representing NULL values
        quote_char: Quote character
        escape_char: Escape character
        
    Returns:
        SQL COPY statement
    """
    sql = f'COPY "{schema}"."{table}"'
    
    if columns:
        column_list = ", ".join([f'"{col}"' for col in columns])
        sql += f" ({column_list})"
    
    sql += f" FROM '{file_path}'"
    
    # Add format options
    options = [
        f"DELIMITER '{delimiter}'",
        f"ENCODING '{encoding}'",
        f"NULL '{null_string}'",
        f"QUOTE '{quote_char}'",
        f"ESCAPE '{escape_char}'"
    ]
    
    if header:
        options.append("HEADER")
    
    sql += f" WITH ({', '.join(options)})"
    
    return sql + ";"