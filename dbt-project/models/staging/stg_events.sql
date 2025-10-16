{{ config(
    materialized="table" 
) }} -- change to incremental later when full data lands

with

source as (

    select * from {{ source('estore_raw', 'events_sampled') }}

),

transformed as (

    select

        event_time,
        event_type,
        cast(product_id as string) as product_id,
        cast(category_id as string) as category_id,
        price,
        cast(user_id as string) as user_id,
        user_session,
        lower(category_code) as category_code,
        lower(brand) as brand

    from source

),

-- filter out products associated with multiple brands as likely data errors 

multi_brand_product_ids as (

    select product_id

    from transformed

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

    from transformed

    where
        product_id not in (
            select multi_brand_product_ids.product_id
            from multi_brand_product_ids
        )
        and product_id is not null

        {% if is_incremental() %}
        -- later, when full data lands
            and event_date >= date_sub(current_date(), interval 2 day)
        {% endif %}

)

select * from final
