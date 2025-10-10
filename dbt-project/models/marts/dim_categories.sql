

{{ config(materialized="table") }}

with final as (
    select
        
        category_id as raw_category_id,
        category_code,
        split(category_code, '.')[SAFE_OFFSET(0)] as category_lvl_1,
        split(category_code, '.')[SAFE_OFFSET(1)] as category_lvl_2,
        split(category_code, '.')[SAFE_OFFSET(2)] as category_lvl_3,
        split(category_code, '.')[SAFE_OFFSET(3)] as category_lvl_4

    from {{ ref('stg_events') }}
    where category_id is not null
    group by category_id, category_code, category_lvl_1, category_lvl_2, category_lvl_3, category_lvl_4
)

select * from final