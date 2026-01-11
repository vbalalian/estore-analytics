{% macro rate_metric(numerator, denominator, precision=5) %}
    round(safe_divide({{ numerator }}, {{ denominator }}), {{ precision }})
{% endmacro %}
