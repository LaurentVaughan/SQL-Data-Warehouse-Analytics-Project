"""
==========================
Common SQL Query Patterns.
==========================

This module provides high-level query patterns that use the builders from query_builder.
All pattern functions follow verb-based naming conventions for clarity.

Pattern Functions:
- analyze_medallion_layer: Query medallion architecture layers with common filters
- trace_data_lineage: Trace data lineage through the medallion architecture
- check_data_quality: Perform comprehensive data quality checks
- compute_pivot_table: Generate pivot table queries
- compute_running_totals: Create running total calculations
- analyze_lag_lead: Generate lag/lead analysis queries
- analyze_cohort_retention: Generate cohort analysis queries
- detect_time_series_gaps: Find gaps in time series data

Usage:
    from sql.common_queries import (
        analyze_medallion_layer,
        trace_data_lineage,
        check_data_quality
    )
    from sql.query_builder import select_builder
    
    # Analyze silver layer data for a specific date
    query = analyze_medallion_layer(
        layer='silver',
        table='customers',
        business_date='2024-01-01',
        source_system='crm'
    )
"""

from typing import Any, Dict, List, Optional
from sql.query_builder import select_builder


def analyze_medallion_layer(
    layer: str,
    table: str,
    columns: Optional[List[str]] = None,
    business_date: Optional[str] = None,
    source_system: Optional[str] = None,
    batch_id: Optional[str] = None,
    include_audit_columns: bool = False,
    order_by_latest: bool = True
) -> str:
    """
    Build a query optimized for medallion architecture patterns.
    
    Args:
        layer: Medallion layer (bronze, silver, gold)
        table: Table name
        columns: Business columns to select
        business_date: Filter by business date
        source_system: Filter by source system
        batch_id: Filter by specific batch
        include_audit_columns: Include audit/metadata columns
        order_by_latest: Order by creation time (latest first)
    
    Returns:
        SQL query optimized for medallion architecture
    """
    # Default columns if not specified
    if columns is None:
        columns = ["*"]
    
    # Add audit columns if requested
    if include_audit_columns:
        audit_columns = [
            'created_at', 'updated_at', 'created_by', 'updated_by',
            'source_system', 'batch_id', 'row_hash'
        ]
        if columns != ["*"]:
            columns.extend([col for col in audit_columns if col not in columns])
    
    # Build WHERE conditions
    where_conditions = []
    
    # Always filter out deleted records
    where_conditions.append('"is_deleted" = FALSE')
    
    # Add business date filter
    if business_date:
        where_conditions.append(f'"created_at"::date = \'{business_date}\'')
    
    # Add source system filter
    if source_system:
        where_conditions.append(f'"source_system" = \'{source_system}\'')
    
    # Add batch filter
    if batch_id:
        where_conditions.append(f'"batch_id" = \'{batch_id}\'')
    
    # Build ORDER BY clause
    order_by = []
    if order_by_latest:
        order_by.append('"created_at" DESC')
    
    # Use the select_builder to construct the query
    return select_builder(
        schema=layer,
        table=table,
        columns=columns,
        where_conditions=where_conditions,
        order_by=order_by,
        include_metadata_filter=False  # We're handling this manually
    )


def trace_data_lineage(
    target_schema: str,
    target_table: str,
    include_upstream: bool = True,
    include_downstream: bool = True,
    max_depth: int = 3
) -> str:
    """
    Build a query to trace data lineage through the medallion architecture.
    
    Args:
        target_schema: Target schema
        target_table: Target table
        include_upstream: Include upstream dependencies
        include_downstream: Include downstream dependencies
        max_depth: Maximum depth for recursive lineage
    
    Returns:
        SQL query for data lineage analysis
    """
    sql = f'''WITH RECURSIVE lineage_tree AS (
    -- Base case: direct lineage for the target table
    SELECT 
        lineage_id,
        source_schema,
        source_table,
        target_schema,
        target_table,
        transformation_logic,
        1 as depth,
        CASE 
            WHEN source_schema = '{target_schema}' AND source_table = '{target_table}' THEN 'upstream'
            WHEN target_schema = '{target_schema}' AND target_table = '{target_table}' THEN 'downstream'
        END as direction
    FROM logs.data_lineage
    WHERE 
        (source_schema = '{target_schema}' AND source_table = '{target_table}')
        OR (target_schema = '{target_schema}' AND target_table = '{target_table}')
    
    UNION ALL
    
    -- Recursive case: follow the lineage chain
    SELECT 
        dl.lineage_id,
        dl.source_schema,
        dl.source_table,
        dl.target_schema,
        dl.target_table,
        dl.transformation_logic,
        lt.depth + 1,
        lt.direction
    FROM logs.data_lineage dl
    INNER JOIN lineage_tree lt ON (
        (lt.direction = 'upstream' AND dl.target_schema = lt.source_schema AND dl.target_table = lt.source_table)
        OR (lt.direction = 'downstream' AND dl.source_schema = lt.target_schema AND dl.source_table = lt.target_table)
    )
    WHERE lt.depth < {max_depth}
)
SELECT DISTINCT
    direction,
    depth,
    source_schema,
    source_table,
    target_schema,
    target_table,
    transformation_logic
FROM lineage_tree'''
    
    # Add direction filters
    direction_filters = []
    if include_upstream:
        direction_filters.append("direction = 'upstream'")
    if include_downstream:
        direction_filters.append("direction = 'downstream'")
    
    if direction_filters:
        sql += f"\nWHERE {' OR '.join(direction_filters)}"
    
    sql += "\nORDER BY direction, depth, source_schema, source_table"
    
    return sql + ";"


def check_data_quality(
    schema: str,
    table: str,
    checks: List[Dict[str, Any]]
) -> str:
    """
    Build a data quality check query.
    
    Args:
        schema: Schema name
        table: Table name
        checks: List of quality check definitions
    
    Returns:
        SQL query that performs multiple data quality checks
    """
    check_clauses = []
    
    for i, check in enumerate(checks):
        check_name = check.get('name', f'check_{i+1}')
        check_type = check['type']
        
        if check_type == 'null_check':
            column = check['column']
            clause = f"""
        '{check_name}' as check_name,
        'null_check' as check_type,
        '{column}' as column_name,
        COUNT(CASE WHEN "{column}" IS NULL THEN 1 END) as null_count,
        COUNT(*) as total_count,
        ROUND(
            COUNT(CASE WHEN "{column}" IS NULL THEN 1 END) * 100.0 / COUNT(*), 2
        ) as null_percentage"""
            check_clauses.append(clause)
        
        elif check_type == 'duplicate_check':
            columns = check['columns']
            column_list = ", ".join([f'"{col}"' for col in columns])
            clause = f"""
        '{check_name}' as check_name,
        'duplicate_check' as check_type,
        '{", ".join(columns)}' as column_name,
        COUNT(*) - COUNT(DISTINCT {column_list}) as duplicate_count,
        COUNT(*) as total_count,
        ROUND(
            (COUNT(*) - COUNT(DISTINCT {column_list})) * 100.0 / COUNT(*), 2
        ) as duplicate_percentage"""
            check_clauses.append(clause)
        
        elif check_type == 'range_check':
            column = check['column']
            min_val = check.get('min_value')
            max_val = check.get('max_value')
            condition = []
            if min_val is not None:
                condition.append(f'"{column}" < {min_val}')
            if max_val is not None:
                condition.append(f'"{column}" > {max_val}')
            
            if condition:
                condition_str = " OR ".join(condition)
                clause = f"""
        '{check_name}' as check_name,
        'range_check' as check_type,
        '{column}' as column_name,
        COUNT(CASE WHEN {condition_str} THEN 1 END) as out_of_range_count,
        COUNT(*) as total_count,
        ROUND(
            COUNT(CASE WHEN {condition_str} THEN 1 END) * 100.0 / COUNT(*), 2
        ) as out_of_range_percentage"""
                check_clauses.append(clause)
    
    if not check_clauses:
        return "SELECT 'No checks defined' as message;"
    
    # Build the final query
    union_separator = ',\n    UNION ALL\n    SELECT\n        '
    sql = f"""WITH quality_checks AS (
    SELECT
        {union_separator.join(check_clauses)}
    FROM "{schema}"."{table}"
    WHERE "is_deleted" = FALSE
)
SELECT 
    check_name,
    check_type,
    column_name,
    COALESCE(null_count, duplicate_count, out_of_range_count) as issue_count,
    total_count,
    COALESCE(null_percentage, duplicate_percentage, out_of_range_percentage) as issue_percentage,
    CASE 
        WHEN COALESCE(null_percentage, duplicate_percentage, out_of_range_percentage) = 0 THEN 'PASS'
        WHEN COALESCE(null_percentage, duplicate_percentage, out_of_range_percentage) <= 5 THEN 'WARNING'
        ELSE 'FAIL'
    END as check_status
FROM quality_checks
ORDER BY issue_percentage DESC"""
    
    return sql + ";"


def compute_pivot_table(
    source_schema: str,
    source_table: str,
    row_columns: List[str],
    pivot_column: str,
    value_column: str,
    pivot_values: List[str],
    aggregate_function: str = "SUM"
) -> str:
    """
    Generate a pivot table query.
    
    Args:
        source_schema: Source schema name
        source_table: Source table name
        row_columns: Columns that will become rows
        pivot_column: Column whose values become new columns
        value_column: Column whose values are aggregated
        pivot_values: List of values to pivot on
        aggregate_function: Aggregation function (SUM, COUNT, AVG, etc.)
        
    Returns:
        Pivot query using conditional aggregation
    """
    row_clause = ", ".join([f'"{col}"' for col in row_columns])
    
    # Build pivot columns using conditional aggregation
    pivot_columns = []
    for value in pivot_values:
        safe_alias = value.replace(' ', '_').replace('-', '_').lower()
        pivot_columns.append(
            f'{aggregate_function}(CASE WHEN "{pivot_column}" = \'{value}\' THEN "{value_column}" END) AS "{safe_alias}"'
        )
    
    pivot_clause = ",\n    ".join(pivot_columns)
    
    sql = f'''SELECT 
    {row_clause},
    {pivot_clause}
FROM "{source_schema}"."{source_table}"
WHERE "is_deleted" = FALSE
GROUP BY {row_clause}
ORDER BY {row_clause}'''
    
    return sql + ";"


def compute_running_totals(
    schema: str,
    table: str,
    value_column: str,
    date_column: str,
    partition_columns: Optional[List[str]] = None,
    reset_frequency: Optional[str] = None
) -> str:
    """
    Generate running totals query with optional partitioning and reset.
    
    Args:
        schema: Schema name
        table: Table name
        value_column: Column to sum
        date_column: Date column for ordering
        partition_columns: Columns to partition by
        reset_frequency: Reset frequency (YEAR, MONTH, QUARTER)
        
    Returns:
        Query with running totals
    """
    partition_clause = ""
    if partition_columns:
        partition_list = ", ".join([f'"{col}"' for col in partition_columns])
        partition_clause = f"PARTITION BY {partition_list}"
    
    # Add reset frequency to partitioning
    if reset_frequency:
        if reset_frequency == "YEAR":
            partition_clause += f", EXTRACT(YEAR FROM \"{date_column}\")"
        elif reset_frequency == "MONTH":
            partition_clause += f", EXTRACT(YEAR FROM \"{date_column}\"), EXTRACT(MONTH FROM \"{date_column}\")"
        elif reset_frequency == "QUARTER":
            partition_clause += f", EXTRACT(YEAR FROM \"{date_column}\"), EXTRACT(QUARTER FROM \"{date_column}\")"
    
    sql = f'''SELECT 
    *,
    SUM("{value_column}") OVER (
        {partition_clause}
        ORDER BY "{date_column}"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    AVG("{value_column}") OVER (
        {partition_clause}
        ORDER BY "{date_column}"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_average
FROM "{schema}"."{table}"
WHERE "is_deleted" = FALSE
ORDER BY {f'{", ".join([f'"{col}"' for col in partition_columns])}, ' if partition_columns else ''}"{date_column}"'''
    
    return sql + ";"


def analyze_lag_lead(
    schema: str,
    table: str,
    value_column: str,
    date_column: str,
    partition_columns: Optional[List[str]] = None,
    periods: List[int] = [1]
) -> str:
    """
    Generate lag/lead analysis query for time series data.
    
    Args:
        schema: Schema name
        table: Table name
        value_column: Column to analyze
        date_column: Date column for ordering
        partition_columns: Columns to partition by
        periods: List of periods to lag/lead (e.g., [1, 7, 30] for 1, 7, 30 periods)
        
    Returns:
        Query with lag/lead analysis
    """
    partition_clause = ""
    if partition_columns:
        partition_list = ", ".join([f'"{col}"' for col in partition_columns])
        partition_clause = f"PARTITION BY {partition_list}"
    
    # Build lag/lead columns
    lag_lead_columns = []
    for period in periods:
        lag_lead_columns.extend([
            f'LAG("{value_column}", {period}) OVER ({partition_clause} ORDER BY "{date_column}") AS lag_{period}_periods',
            f'LEAD("{value_column}", {period}) OVER ({partition_clause} ORDER BY "{date_column}") AS lead_{period}_periods',
            f'"{value_column}" - LAG("{value_column}", {period}) OVER ({partition_clause} ORDER BY "{date_column}") AS change_from_{period}_periods_ago',
            f'ROUND(100.0 * ("{value_column}" - LAG("{value_column}", {period}) OVER ({partition_clause} ORDER BY "{date_column}")) / NULLIF(LAG("{value_column}", {period}) OVER ({partition_clause} ORDER BY "{date_column}"), 0), 2) AS pct_change_from_{period}_periods_ago'
        ])
    
    lag_lead_clause = ",\n    ".join(lag_lead_columns)
    
    sql = f'''SELECT 
    *,
    {lag_lead_clause}
FROM "{schema}"."{table}"
WHERE "is_deleted" = FALSE
ORDER BY {f'{", ".join([f'"{col}"' for col in partition_columns])}, ' if partition_columns else ''}"{date_column}"'''
    
    return sql + ";"


def analyze_cohort_retention(
    schema: str,
    table: str,
    user_id_column: str,
    event_date_column: str,
    cohort_date_column: str,
    metric_column: Optional[str] = None,
    periods: int = 12
) -> str:
    """
    Generate cohort analysis query.
    
    Args:
        schema: Schema name
        table: Table name
        user_id_column: User identifier column
        event_date_column: Event date column
        cohort_date_column: First activity date column
        metric_column: Optional metric to track (revenue, etc.)
        periods: Number of periods to analyze
        
    Returns:
        Cohort analysis query
    """
    metric_calc = f'SUM("{metric_column}")' if metric_column else 'COUNT(DISTINCT "user_id")'
    
    sql = f'''WITH cohort_data AS (
    SELECT 
        "{user_id_column}",
        "{cohort_date_column}"::date as cohort_month,
        "{event_date_column}"::date as event_month,
        EXTRACT(YEAR FROM AGE("{event_date_column}"::date, "{cohort_date_column}"::date)) * 12 +
        EXTRACT(MONTH FROM AGE("{event_date_column}"::date, "{cohort_date_column}"::date)) as period_number
        {f', "{metric_column}"' if metric_column else ''}
    FROM "{schema}"."{table}"
    WHERE "is_deleted" = FALSE
),
cohort_table AS (
    SELECT 
        cohort_month,
        period_number,
        {metric_calc} as cohort_value
    FROM cohort_data
    WHERE period_number BETWEEN 0 AND {periods - 1}
    GROUP BY cohort_month, period_number
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        cohort_value as cohort_size
    FROM cohort_table
    WHERE period_number = 0
)
SELECT 
    ct.cohort_month,
    ct.period_number,
    ct.cohort_value,
    cs.cohort_size,
    ROUND(100.0 * ct.cohort_value / cs.cohort_size, 2) as retention_rate
FROM cohort_table ct
LEFT JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
ORDER BY ct.cohort_month, ct.period_number'''
    
    return sql + ";"


def detect_time_series_gaps(
    schema: str,
    table: str,
    date_column: str,
    partition_columns: Optional[List[str]] = None,
    expected_frequency: str = "daily"
) -> str:
    """
    Find gaps in time series data.
    
    Args:
        schema: Schema name
        table: Table name
        date_column: Date column to check for gaps
        partition_columns: Columns that define separate time series
        expected_frequency: Expected frequency (daily, weekly, monthly)
        
    Returns:
        Query that identifies gaps in time series
    """
    # Determine the interval based on frequency
    interval_map = {
        "daily": "1 day",
        "weekly": "1 week", 
        "monthly": "1 month"
    }
    interval = interval_map.get(expected_frequency, "1 day")
    
    partition_clause = ""
    partition_select = ""
    if partition_columns:
        partition_list = ", ".join([f'"{col}"' for col in partition_columns])
        partition_clause = f"PARTITION BY {partition_list}"
        partition_select = f"{partition_list}, "
    
    sql = f'''WITH date_series AS (
    SELECT 
        {partition_select}
        "{date_column}"::date as current_date,
        LEAD("{date_column}"::date) OVER ({partition_clause} ORDER BY "{date_column}") as next_date
    FROM "{schema}"."{table}"
    WHERE "is_deleted" = FALSE
),
gaps AS (
    SELECT 
        {partition_select}
        current_date,
        next_date,
        next_date - current_date as gap_days,
        CASE 
            WHEN next_date - current_date > INTERVAL '{interval}' THEN true
            ELSE false
        END as has_gap
    FROM date_series
    WHERE next_date IS NOT NULL
)
SELECT 
    {partition_select}
    current_date as gap_start,
    next_date as gap_end,
    gap_days,
    gap_days - INTERVAL '{interval}' as gap_length
FROM gaps
WHERE has_gap = true
ORDER BY {partition_select}current_date'''
    
    return sql + ";"