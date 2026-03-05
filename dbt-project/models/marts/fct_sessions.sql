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
            >= (select max(t.session_start_date) from {{ this }} as t)
    {% endif %}
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
  from fct_events
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
    seconds_since_prev_event,
    case
      when seconds_since_prev_event > 1800
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
    seconds_since_prev_event,
    is_timeout,
    sum(is_timeout) over (
      partition by user_session
      order by event_time
      rows between unbounded preceding and current row
    ) as timeout_occurence
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
      when timeout_occurence = 0 then user_session
      else concat(
        user_session,
        '_timeout_',
        cast(timeout_occurence as string)
      )
    end as cleaned_session_id
  from sub_sessions
),

stg_sessions as (
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
            as session_length
    from events_with_cleaned_id
    group by cleaned_session_id
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
        stg_sessions.session_length,

        case when stg_sessions.view_count > 0 then 1 else 0 end
            as reached_view,
        case when stg_sessions.cart_additions > 0 then 1 else 0 end
            as reached_cart,
        case when stg_sessions.purchase_count > 0 then 1 else 0 end
            as reached_purchase,

        case
            when stg_sessions.purchase_count > 0 then 'purchase'
            when stg_sessions.cart_additions > 0 then 'cart'
            when stg_sessions.view_count > 0 then 'view'
            else 'no_activity'
        end as funnel_stage

    from stg_sessions

    {% if is_incremental() %}
        left join existing_sessions
            on stg_sessions.session_id = existing_sessions.session_id
    {% endif %}
)

select * from final_sessions
