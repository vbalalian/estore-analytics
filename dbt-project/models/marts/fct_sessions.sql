{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    partition_by = {
    "field": "session_start_date", 
    "data_type": "date",
    "granularity": "day"
    },
    cluster_by = ['user_id', 'session_start_date'],
    incremental_strategy = 'merge'
) }}

with
{% if is_incremental() %}
    existing_sessions as (
        select
            session_id,
            session_start_date,
            session_start_time
        from {{ this }}
    ),
{% endif %}
fct_events as (
    select * from {{ ref('fct_events') }}
    {% if is_incremental() %}
        where
            fct_events.event_date
            >= date_sub(
                (select max(t.session_start_date) from {{ this }} as t),
                interval 2 day
            )
    {% endif %}
),

stg_sessions as (
    select
        user_session as session_id,
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
        max(is_purchase) as converted,
        datetime_diff(max(event_time), min(event_time), second)
            as session_length
    from fct_events
    group by user_session
),

final_sessions as (
    select
        stg_sessions.session_id,
        {% if is_incremental() %}
            coalesce(
                existing_sessions.session_start_time,
                stg_sessions.session_start_time
            ) as session_start_time,
            coalesce(
                existing_sessions.session_start_date,
                stg_sessions.session_start_date
            ) as session_start_date,
        {% else %}
        stg_sessions.session_start_time,
        stg_sessions.session_start_date,
        {% endif %}
        stg_sessions.user_id,
        stg_sessions.session_end_time,
        stg_sessions.event_count,
        stg_sessions.unique_product_count,
        stg_sessions.cart_additions,
        stg_sessions.purchase_count,
        stg_sessions.view_count,
        stg_sessions.total_revenue,
        stg_sessions.converted,
        stg_sessions.session_length
    from stg_sessions
    {% if is_incremental() %}
        left join existing_sessions
            on stg_sessions.session_id = existing_sessions.session_id
    {% endif %}
)

select * from final_sessions
