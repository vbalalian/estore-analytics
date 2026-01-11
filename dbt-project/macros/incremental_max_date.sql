{% macro incremental_max_date_cte(model, date_column='event_date', cte_name='max_event_date') %}
{% if is_incremental() %}
    {{ cte_name }} as (

        select max({{ date_column }}) as max_date

        from {{ ref(model) }}
    ),
{% endif %}
{% endmacro %}


{% macro incremental_date_filter(date_column='event_date', cte_name='max_event_date') %}
{% if is_incremental() %}

        where
            {{ date_column }}
            >= (
                select date({{ cte_name }}.max_date)
                from {{ cte_name }}
            )

{% endif %}
{% endmacro %}
