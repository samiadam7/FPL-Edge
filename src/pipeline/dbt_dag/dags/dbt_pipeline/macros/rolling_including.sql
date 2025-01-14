{% macro rolling_stat_including(column, partition_by, order_by, start_row) %}
    SUM({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ start_row }} AND CURRENT ROW
    )
{% endmacro %}
