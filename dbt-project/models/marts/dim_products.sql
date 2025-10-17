{{ config(
    materialized = 'incremental',
    unique_key = 'product_id',
    cluster_by = ['product_id', 'category_lvl_1', 'brand'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

events_source as (

    select * from {{ ref('stg_events') }}

),

categories as (

    select * from {{ ref('dim_categories') }}

),

product_attributes as (

    select

        product_id,
        brand,
        category_id,
        count(*) as frequency

    from events_source

    group by product_id, brand, category_id

),

distinct_products_ranked as (

    select

        *,
        row_number()
            over (partition by product_id order by frequency desc)
            as rank

    from product_attributes

),

final as (

    select

        distinct_products_ranked.product_id,
        distinct_products_ranked.brand,
        categories.category_code,
        categories.category_lvl_1,
        categories.category_lvl_2,
        categories.category_lvl_3,
        categories.category_lvl_4

    from distinct_products_ranked

    left join categories
        on distinct_products_ranked.category_id = categories.raw_category_id

    where distinct_products_ranked.rank = 1

)

select * from final
