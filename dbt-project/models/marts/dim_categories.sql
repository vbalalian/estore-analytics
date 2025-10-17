{{ config(
    materialized = 'incremental',
    unique_key = 'raw_category_id',
    cluster_by = ['category_lvl_1', 'category_lvl_2'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

{%  if is_incremental() %}
max_event_date as (

    select

        max(event_date) as max_date

    from {{ ref('stg_events') }}
),
{% endif %}

source as (

    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}

        where event_date >= (select date_sub(date(max_date), interval 2 day) from max_event_date)

    {% endif %}

),

grouped as (

    select

        category_id,
        ANY_VALUE(category_code) as category_code

    from source

    where category_id is not null

    group by category_id

),

final as (

    select

        category_id as raw_category_id,
        category_code,
        split(category_code, '.')[safe_offset(0)] as category_lvl_1,
        split(category_code, '.')[safe_offset(1)] as category_lvl_2,
        split(category_code, '.')[safe_offset(2)] as category_lvl_3,
        split(category_code, '.')[safe_offset(3)] as category_lvl_4

    from grouped

)

select * from final
