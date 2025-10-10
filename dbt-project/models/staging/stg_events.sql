
{{ config(
    materialized="table"  -- simple now
    -- later: switch to incremental + partition_by
) }}

with source as (

    select
        event_time,
        event_type,
        cast(product_id as string) as product_id,
        cast(category_id as string) as category_id,
        lower(category_code) as category_code,
        lower(brand) as brand,
        price,
        cast(user_id as string) as user_id,
        user_session
    from {{ source('estore_raw', 'events_sampled') }}

),

-- filter out products associated with multiple brands since they're likely data errors 

multi_brand_products as (

    select
        product_id
    from source
    where brand is not null
    group by product_id
    having count(distinct brand) > 1
        
),

final as (

    select
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,
        cast(event_time as date) as event_date
    from source
    where product_id not in (select product_id from multi_brand_products)
    and product_id is not null

    {% if is_incremental() %}
        -- later, when full data lands
        and event_date >= date_sub(current_date(), interval 2 day)
    {% endif %}

)

select * from final
