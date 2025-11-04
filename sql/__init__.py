"""
====================================================
SQL utilities package for data warehouse operations.
====================================================

This package provides modular, reusable SQL construction functions organized
by SQL operation type. All functions generate parameterized SQL strings that
can be executed via SQLAlchemy or other database drivers.

The package follows a clear organization:
    - ddl.py: Data Definition Language (CREATE/ALTER/DROP schemas and tables)
    - dml.py: Data Manipulation Language (INSERT/UPDATE/DELETE/MERGE)
    - query_builder.py: Query builders and metadata queries (_builder suffix)
    - common_queries.py: High-level query patterns (verb-based prefixes)

Architecture:
    - All builders end with '_builder' suffix (e.g., select_builder, cte_builder)
    - Common queries use verb prefixes (analyze_, compute_, detect_, check_, trace_)
    - common_queries.py imports from query_builder.py (not vice versa)
    - All SQL generation is pure functions (no side effects)

Example:
    >>> from sql.ddl import create_schema, create_table
    >>> from sql.dml import bulk_insert, upsert
    >>> from sql.query_builder import select_builder
    >>> from sql.common_queries import analyze_medallion_layer
    >>> 
    >>> # Create a schema
    >>> schema_sql = create_schema('bronze', comment='Raw data layer')
    >>> 
    >>> # Build a SELECT query
    >>> query = select_builder(
    ...     table='customers',
    ...     columns=['id', 'name', 'email'],
    ...     where_clauses=['status = :status'],
    ...     order_by=['created_at DESC']
    ... )
"""

__version__ = "1.0.0"
__all__ = [
    # DDL functions
    'create_table', 'create_schema', 'create_index', 'create_constraint',
    'create_medallion_table_template',
    # DML functions  
    'bulk_insert', 'upsert', 'soft_delete', 'batch_update',
    'merge_statement', 'incremental_load'
]

from .ddl import (
    create_constraint,
    create_index,
    create_medallion_table_template,
    create_schema,
    create_table,
)
from .dml import (
    batch_update,
    bulk_insert,
    incremental_load,
    merge_statement,
    soft_delete,
    upsert,
)
