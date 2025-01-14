{% macro rolling_stat(column, partition_by, order_by, start_row) %}
    SUM({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ start_row }} AND 1 PRECEDING
    )
{% endmacro %}
