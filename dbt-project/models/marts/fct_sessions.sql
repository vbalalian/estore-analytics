with

fct_events as (

    select * from {{ ref('fct_events') }}

),

stg_sessions as (

    select

        user_session as session_id,
        user_id,
        min(event_time) as session_start,
        max(event_time) as session_end,
        count(*) as total_events,
        sum(is_cart_add) as cart_additions,
        sum(is_purchase) as total_purchases,
        sum(is_view) as total_views,
        sum(revenue) as total_revenue,
        max(is_purchase) as converted

    from fct_events

    group by user_session, user_id

),

final_sessions as (

    select

        session_id,
        user_id,
        session_start,
        session_end,
        total_events,
        cart_additions,
        total_purchases,
        total_views,
        total_revenue,
        converted,
        datetime_diff(session_end, session_start, second) as session_length

    from stg_sessions

)

select * from final_sessions
