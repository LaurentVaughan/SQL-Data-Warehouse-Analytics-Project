"""
============================
SQL Query Builder Utilities.
============================

This module provides low-level building blocks for constructing SQL queries.
All builders follow the _builder naming convention for consistency.

Query Builders:
- select_builder: Build SELECT statements with flexible column selection
- join_builder: Construct JOIN clauses with proper table aliases
- where_builder: Build dynamic WHERE conditions
- pagination_builder: Add LIMIT and OFFSET for pagination
- cte_builder: Build Common Table Expressions
- window_function_builder: Generate window function clauses
- subquery_builder: Create correlated and non-correlated subqueries
- recursive_cte_builder: Build recursive CTEs for hierarchical data

Metadata Query Functions:
- check_schema_exists_sql: Check if a schema exists
- get_schema_info_sql: Get schema information
- get_table_info_sql: Get table information
- get_column_info_sql: Get column information
- get_database_info_sql: Get database information
- get_table_stats_sql: Get table statistics
- check_database_exists_sql: Check if a database exists
- count_database_connections_sql: Count database connections

Usage:
    from sql.query_builder import (
        select_builder, join_builder, where_builder,
        cte_builder, window_function_builder
    )
    
    # Build a complex query with CTEs
    cte_query = cte_builder(
        ctes=[{'name': 'monthly_sales', 'query': 'SELECT ... FROM sales'}],
        main_query=select_builder(
            schema='silver',
            table='customers',
            columns=['customer_id', 'customer_name', 'email']
        )
    )
"""

from typing import Any, Dict, List, Optional, Union


def select_builder(
    schema: str,
    table: str,
    columns: Union[List[str], str] = "*",
    joins: Optional[List[Dict[str, str]]] = None,
    where_conditions: Optional[List[str]] = None,
    group_by: Optional[List[str]] = None,
    having_conditions: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    distinct: bool = False,
    include_metadata_filter: bool = True,
    table_alias: Optional[str] = None
) -> str:
    """
    Build a SELECT statement with optional joins, filtering, and ordering.
    
    Args:
        schema: Schema name
        table: Main table name
        columns: Column list or "*" for all columns
        joins: List of join definitions
        where_conditions: List of WHERE conditions
        group_by: List of GROUP BY columns
        having_conditions: List of HAVING conditions
        order_by: List of ORDER BY expressions
        limit: LIMIT clause value
        offset: OFFSET clause value
        distinct: Use SELECT DISTINCT
        include_metadata_filter: Add standard medallion filters
        table_alias: Alias for the main table
        
    Returns:
        SQL SELECT statement
    """
    # Build the SELECT clause
    select_keyword = "SELECT DISTINCT" if distinct else "SELECT"
    
    # Handle columns
    if isinstance(columns, str):
        column_clause = columns
    else:
        # Add table alias prefix if specified
        if table_alias:
            column_clause = ", ".join([
                f"{table_alias}.{col}" if not col.startswith(table_alias) and '.' not in col 
                else col for col in columns
            ])
        else:
            column_clause = ", ".join([f'"{col}"' for col in columns])
    
    # Build the FROM clause
    main_table = f'"{schema}"."{table}"'
    if table_alias:
        main_table += f' AS {table_alias}'
    
    sql = f"""{select_keyword} {column_clause}
FROM {main_table}"""
    
    # Add JOINs
    if joins:
        for join in joins:
            sql += "\n" + join_builder(join)
    
    # Build WHERE clause
    all_where_conditions = []
    
    # Add user-specified conditions
    if where_conditions:
        all_where_conditions.extend(where_conditions)
    
    # Add metadata filters for medallion architecture
    if include_metadata_filter:
        table_prefix = f"{table_alias}." if table_alias else f'"{table}".'
        all_where_conditions.append(f'{table_prefix}"is_deleted" = FALSE')
    
    if all_where_conditions:
        sql += "\nWHERE " + "\n  AND ".join(all_where_conditions)
    
    # Add GROUP BY
    if group_by:
        group_clause = ", ".join([f'"{col}"' for col in group_by])
        sql += f"\nGROUP BY {group_clause}"
    
    # Add HAVING
    if having_conditions:
        having_clause = " AND ".join(having_conditions)
        sql += f"\nHAVING {having_clause}"
    
    # Add ORDER BY
    if order_by:
        order_clause = ", ".join(order_by)
        sql += f"\nORDER BY {order_clause}"
    
    # Add LIMIT and OFFSET
    if limit:
        sql += f"\nLIMIT {limit}"
    
    if offset:
        sql += f"\nOFFSET {offset}"
    
    return sql + ";"


def join_builder(join_config: Dict[str, str]) -> str:
    """
    Build a JOIN clause from configuration.
    
    Args:
        join_config: Dictionary with join configuration
            - table: Table name (can include schema)
            - type: JOIN type (INNER, LEFT, RIGHT, FULL)
            - on: JOIN condition
            - alias: Optional table alias
            - schema: Optional schema (if not in table name)
    
    Returns:
        SQL JOIN clause
    """
    join_type = join_config.get('type', 'INNER').upper()
    table = join_config['table']
    
    # Add schema if specified separately
    if 'schema' in join_config and '.' not in table:
        table = f'"{join_config["schema"]}"."{table}"'
    elif '.' not in table:
        table = f'"{table}"'
    
    # Add alias if specified
    if 'alias' in join_config:
        table += f' AS {join_config["alias"]}'
    
    join_clause = f"{join_type} JOIN {table}"
    
    if 'on' in join_config:
        join_clause += f" ON {join_config['on']}"
    
    return join_clause


def where_builder(
    conditions: List[Union[str, Dict[str, Any]]],
    operator: str = "AND"
) -> str:
    """
    Build WHERE clause from conditions.
    
    Args:
        conditions: List of conditions (strings or dictionaries)
        operator: Logical operator between conditions (AND, OR)
    
    Returns:
        WHERE clause without the WHERE keyword
    """
    processed_conditions = []
    
    for condition in conditions:
        if isinstance(condition, str):
            processed_conditions.append(condition)
        elif isinstance(condition, dict):
            # Handle dictionary-based conditions
            if 'column' in condition and 'value' in condition:
                column = condition['column']
                value = condition['value']
                op = condition.get('operator', '=')
                
                if isinstance(value, str):
                    processed_conditions.append(f'"{column}" {op} \'{value}\'')
                elif isinstance(value, list):
                    value_list = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in value])
                    processed_conditions.append(f'"{column}" IN ({value_list})')
                else:
                    processed_conditions.append(f'"{column}" {op} {value}')
    
    return f" {operator} ".join(processed_conditions)


def pagination_builder(page: int, page_size: int) -> Dict[str, int]:
    """
    Calculate LIMIT and OFFSET for pagination.
    
    Args:
        page: Page number (1-based)
        page_size: Number of records per page
    
    Returns:
        Dictionary with limit and offset values
    """
    offset = (page - 1) * page_size
    return {
        'limit': page_size,
        'offset': offset
    }


def cte_builder(
    ctes: List[Dict[str, str]],
    main_query: str,
    recursive: bool = False
) -> str:
    """
    Build a query with Common Table Expressions (CTEs).
    
    Args:
        ctes: List of CTE definitions with 'name' and 'query'
        main_query: Main SELECT query that uses the CTEs
        recursive: Whether to use WITH RECURSIVE
        
    Returns:
        Complete SQL query with CTEs
    """
    with_keyword = "WITH RECURSIVE" if recursive else "WITH"
    
    cte_clauses = []
    for cte in ctes:
        cte_clause = f'{cte["name"]} AS (\n    {cte["query"]}\n)'
        cte_clauses.append(cte_clause)
    
    # Join CTEs with comma and newline
    cte_text = ',\n'.join(cte_clauses)
    
    sql = f"""{with_keyword}
{cte_text}
{main_query}"""
    
    return sql


def window_function_builder(
    function_name: str,
    column: Optional[str] = None,
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    frame_clause: Optional[str] = None,
    alias: Optional[str] = None
) -> str:
    """
    Generate window function clause.
    
    Args:
        function_name: Window function name (ROW_NUMBER, RANK, SUM, etc.)
        column: Column to apply function to (for aggregate functions)
        partition_by: PARTITION BY columns
        order_by: ORDER BY columns
        frame_clause: Window frame clause (e.g., 'ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING')
        alias: Column alias
        
    Returns:
        Window function clause
    """
    # Build the function call
    if column:
        func_call = f'{function_name}("{column}")'
    else:
        func_call = f'{function_name}()'
    
    # Build the OVER clause
    over_parts = []
    
    if partition_by:
        partition_clause = ", ".join([f'"{col}"' for col in partition_by])
        over_parts.append(f"PARTITION BY {partition_clause}")
    
    if order_by:
        order_clause = ", ".join(order_by)
        over_parts.append(f"ORDER BY {order_clause}")
    
    if frame_clause:
        over_parts.append(frame_clause)
    
    over_clause = " ".join(over_parts)
    
    result = f"{func_call} OVER ({over_clause})"
    
    if alias:
        result += f" AS {alias}"
    
    return result


def subquery_builder(
    subquery: str,
    alias: str,
    correlation_type: Optional[str] = None,
    where_clause: Optional[str] = None
) -> str:
    """
    Build a subquery with proper aliasing and correlation.
    
    Args:
        subquery: The subquery SQL
        alias: Alias for the subquery
        correlation_type: Type of correlation (EXISTS, NOT EXISTS, IN, NOT IN)
        where_clause: Additional WHERE clause for the subquery
        
    Returns:
        Formatted subquery
    """
    formatted_subquery = f"(\n    {subquery}"
    
    if where_clause:
        formatted_subquery += f"\n    AND {where_clause}"
    
    formatted_subquery += f"\n) AS {alias}"
    
    if correlation_type:
        return f"{correlation_type} {formatted_subquery}"
    
    return formatted_subquery


def recursive_cte_builder(
    cte_name: str,
    base_query: str,
    recursive_query: str,
    union_type: str = "UNION ALL"
) -> str:
    """
    Build a recursive CTE for hierarchical data.
    
    Args:
        cte_name: Name of the recursive CTE
        base_query: Base case query
        recursive_query: Recursive case query
        union_type: UNION or UNION ALL
        
    Returns:
        Recursive CTE definition
    """
    return f'''{cte_name} AS (
    -- Base case
    {base_query}
    
    {union_type}
    
    -- Recursive case
    {recursive_query}
)'''


def check_schema_exists_sql(schema_name: str) -> str:
    """
    Generate SQL to check if a schema exists.
    
    Args:
        schema_name: Name of the schema to check
        
    Returns:
        SQL query that returns 1 if schema exists, nothing if not
    """
    return f"""SELECT 1 
FROM information_schema.schemata 
WHERE schema_name = '{schema_name}'"""


def get_schema_info_sql(schema_names: Optional[List[str]] = None) -> str:
    """
    Generate SQL to get information about schemas.
    
    Args:
        schema_names: Optional list of schema names to filter by
        
    Returns:
        SQL query to retrieve schema information
    """
    sql = """SELECT 
    schema_name,
    schema_owner,
    obj_description(oid, 'pg_namespace') as description
FROM information_schema.schemata s
LEFT JOIN pg_namespace n ON s.schema_name = n.nspname"""
    
    if schema_names:
        # Create IN clause with proper escaping
        schema_list = "', '".join(schema_names)
        sql += f"\nWHERE schema_name IN ('{schema_list}')"
    
    sql += "\nORDER BY schema_name"
    
    return sql


def get_table_info_sql(
    schema_name: Optional[str] = None,
    table_pattern: Optional[str] = None
) -> str:
    """
    Generate SQL to get information about tables.
    
    Args:
        schema_name: Optional schema name to filter by
        table_pattern: Optional LIKE pattern for table names
        
    Returns:
        SQL query to retrieve table information
    """
    sql = """SELECT 
    t.table_schema,
    t.table_name,
    t.table_type,
    pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))) as total_size,
    pg_size_pretty(pg_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))) as table_size,
    obj_description((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass, 'pg_class') as description
FROM information_schema.tables t
WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')"""
    
    if schema_name:
        sql += f"\n  AND t.table_schema = '{schema_name}'"
    
    if table_pattern:
        sql += f"\n  AND t.table_name LIKE '{table_pattern}'"
    
    sql += "\nORDER BY t.table_schema, t.table_name"
    
    return sql


def get_column_info_sql(schema_name: str, table_name: str) -> str:
    """
    Generate SQL to get information about table columns.
    
    Args:
        schema_name: Schema name
        table_name: Table name
        
    Returns:
        SQL query to retrieve column information
    """
    return f"""SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default,
    ordinal_position,
    col_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass, ordinal_position) as description
FROM information_schema.columns
WHERE table_schema = '{schema_name}'
  AND table_name = '{table_name}'
ORDER BY ordinal_position"""


def get_database_info_sql(database_name: str) -> str:
    """
    Generate SQL to get information about a database.
    
    Args:
        database_name: Database name
        
    Returns:
        SQL query to retrieve database information
    """
    return f"""SELECT 
    datname as database_name,
    pg_encoding_to_char(encoding) as encoding,
    datcollate as lc_collate,
    datctype as lc_ctype,
    pg_size_pretty(pg_database_size(datname)) as size,
    pg_catalog.pg_get_userbyid(datdba) as owner
FROM pg_database
WHERE datname = '{database_name}'"""


def get_table_stats_sql(schema_name: str, table_name: str) -> str:
    """
    Generate SQL to get statistics about a table.
    
    Args:
        schema_name: Schema name
        table_name: Table name
        
    Returns:
        SQL query to retrieve table statistics
    """
    return f"""SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname = '{schema_name}'
  AND tablename = '{table_name}'"""


def check_database_exists_sql(database_name: str) -> str:
    """
    Generate SQL to check if a database exists.
    
    Args:
        database_name: Name of the database to check
        
    Returns:
        SQL query that returns 1 if database exists, nothing if not
    """
    return f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"


def count_database_connections_sql(database_name: str) -> str:
    """
    Generate SQL to count active connections to a database.
    
    Args:
        database_name: Name of the database
        
    Returns:
        SQL query to count connections
    """
    return f"""SELECT COUNT(*) 
FROM pg_stat_activity 
WHERE datname = '{database_name}' 
  AND pid <> pg_backend_pid()"""