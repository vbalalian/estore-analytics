{% macro one_hot_k_minus_1(column_name, categories, prefix) %}
  {% set baseline = categories[-1] %}  -- drop the last category as baseline
  {% set dummy_cols = [] %}

  {% for cat in categories[:-1] %}
    {% set col_name = prefix ~ '_' ~ cat | lower | replace(' ', '_') %}
    {% do dummy_cols.append('CASE WHEN ' ~ column_name ~ " = '" ~ cat ~ "' THEN 1 ELSE 0 END AS " ~ col_name) %}
  {% endfor %}

  {{ return(dummy_cols | join(',\n    ')) }}
{% endmacro %}
