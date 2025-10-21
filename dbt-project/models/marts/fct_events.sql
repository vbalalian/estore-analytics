{{ config(
    materialized="incremental",
    unique_key="event_id",
    partition_by={
    "field": "event_date", 
    "data_type": "date",
    "granularity": "day"
    },
    incremental_strategy="insert_overwrite"
) }}

with

stg_events as (

    select * from {{ ref('stg_events') }}

    {% if is_incremental() %}

        where
            stg_events.event_date
            >= date_sub(
                (select max(t.event_date) from {{ this }} as t), interval 2 day
            )

    {% endif %}

),

numbered as (

    select
        *,
        row_number() over (
            order by event_time, user_id, user_session, product_id
        ) as _row_num

    from stg_events
),

final_events as (

    select

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
        end as revenue,

        {{ dbt_utils.generate_surrogate_key([
            'event_time',
            'event_type',
            'user_id',
            'product_id',
            'user_session',
            '_row_num'
        ]) }} as event_id

    from numbered

)

select * from final_events
