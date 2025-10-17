{{ config(
    materialized = 'incremental',
    unique_key = 'raw_category_id',
    cluster_by = ['category_lvl_1', 'category_lvl_2', 'category_lvl_3', 'category_lvl_4'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

source as (

    select * from {{ ref('stg_events') }}

),

final as (

    select

        category_id as raw_category_id,
        category_code,
        split(category_code, '.')[safe_offset(0)] as category_lvl_1,
        split(category_code, '.')[safe_offset(1)] as category_lvl_2,
        split(category_code, '.')[safe_offset(2)] as category_lvl_3,
        split(category_code, '.')[safe_offset(3)] as category_lvl_4

    from source

    where category_id is not null

    group by
        category_id,
        category_code,
        category_lvl_1,
        category_lvl_2,
        category_lvl_3,
        category_lvl_4

)

select * from final
