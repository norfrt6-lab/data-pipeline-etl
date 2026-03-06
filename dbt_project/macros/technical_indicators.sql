-- Macro: compute simple moving average in SQL
{% macro sma(column, period, partition_by='symbol', order_by='date') %}
    avg({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ period - 1 }} preceding and current row
    )
{% endmacro %}

-- Macro: compute percentage change
{% macro pct_change(column, periods=1, partition_by='symbol', order_by='date') %}
    ({{ column }} - lag({{ column }}, {{ periods }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    )) / nullif(lag({{ column }}, {{ periods }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    ), 0)
{% endmacro %}

-- Macro: compute rolling standard deviation
{% macro rolling_std(column, period, partition_by='symbol', order_by='date') %}
    stddev({{ column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ period - 1 }} preceding and current row
    )
{% endmacro %}
