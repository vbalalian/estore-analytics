{{ config(
    materialized = 'table',
    partition_by = {
        "field": "session_start_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ['user_id', 'session_start_date']
) }}

with

events as (
    select
        event_time,
        user_session,
        user_id,
        product_id,
        is_cart_add,
        is_purchase,
        is_view,
        revenue
    from {{ ref('fct_events') }}
),

events_with_gaps as (
    select
        event_time,
        user_session,
        user_id,
        product_id,
        is_cart_add,
        is_purchase,
        is_view,
        revenue,
        datetime_diff(
            event_time,
            lag(event_time) over (
                partition by user_session
                order by event_time
            ),
            second
        ) as seconds_since_prev_event
    from events
),

timeout_flags as (
    select
        event_time,
        user_session,
        user_id,
        product_id,
        is_cart_add,
        is_purchase,
        is_view,
        revenue,
        case
            when
                seconds_since_prev_event
                > {{ var('session_timeout_seconds') }}
                then 1
            else 0
        end as is_timeout
    from events_with_gaps
),

sub_sessions as (
    select
        event_time,
        user_session,
        user_id,
        product_id,
        is_cart_add,
        is_purchase,
        is_view,
        revenue,
        sum(is_timeout) over (
            partition by user_session
            order by event_time
            rows between unbounded preceding and current row
        ) as timeout_occurrence
    from timeout_flags
),

events_with_cleaned_id as (
    select
        event_time,
        user_id,
        product_id,
        is_cart_add,
        is_purchase,
        is_view,
        revenue,
        case
            when timeout_occurrence = 0 then user_session
            else concat(
                user_session,
                '_timeout_',
                cast(timeout_occurrence as string)
            )
        end as cleaned_session_id
    from sub_sessions
),

cleaned_sessions as (
    select
        cleaned_session_id as session_id,
        min(event_time) as session_start_time,
        date(min(event_time)) as session_start_date,
        max(user_id) as user_id,
        max(event_time) as session_end_time,
        count(*) as event_count,
        count(distinct product_id) as unique_product_count,
        sum(is_cart_add) as cart_additions,
        sum(is_purchase) as purchase_count,
        sum(is_view) as view_count,
        sum(revenue) as total_revenue,
        datetime_diff(max(event_time), min(event_time), second)
            as session_length,

        case when sum(is_view) > 0 then 1 else 0 end as reached_view,
        case when sum(is_cart_add) > 0 then 1 else 0 end as reached_cart,
        case when sum(is_purchase) > 0 then 1 else 0 end as reached_purchase,

        case
            when sum(is_purchase) > 0 then 'purchase'
            when sum(is_cart_add) > 0 then 'cart'
            when sum(is_view) > 0 then 'view'
            else 'no_activity'
        end as funnel_stage

    from events_with_cleaned_id
    group by cleaned_session_id
)

select * from cleaned_sessions
