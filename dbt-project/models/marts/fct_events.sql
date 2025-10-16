with

stg_events as (

    select * from {{ ref('stg_events') }}

),

final_events as (

    select

        event_id,
        event_time,
        event_date,
        event_type,
        user_id,
        user_session,
        product_id,
        brand,
        category_code,
        category_id,
        price,
        1 as quantity,
        extract(hour from event_time) as event_hour,

        case
            when event_type = 'purchase'
                then 1
            else 0
        end as is_purchase,

        case
            when event_type = 'cart'
                then 1
            else 0
        end as is_cart_add,

        case
            when event_type = 'view'
                then 1
            else 0
        end as is_view,

        case
            when event_type = 'purchase'
                then price
        end as revenue

    from stg_events

)

select * from final_events
