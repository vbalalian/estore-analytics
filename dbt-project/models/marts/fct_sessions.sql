{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    partition_by = {"field": "session_date", "data_type": "date"},
    cluster_by = ['user_id', 'session_date'],
    incremental_strategy = 'insert_overwrite'
) }}

with

fct_events as (

    select * from {{ ref('fct_events') }}

    {% if is_incremental() %}

        where event_date >= date_sub(date(_dbt_max_partition), interval 2 day)

    {% endif %}

),

stg_sessions as (

    select

        user_session as session_id,
        min(event_date) as session_date,
        max(user_id) as user_id,
        min(event_time) as session_start_time,
        max(event_time) as session_end_time,
        count(*) as event_count,
        count(distinct product_id) as unique_product_count,
        sum(is_cart_add) as cart_additions,
        sum(is_purchase) as purchase_count,
        sum(is_view) as view_count,
        sum(revenue) as total_revenue,
        max(is_purchase) as converted,
        datetime_diff(max(event_time), min(event_time), second) as session_length

    from fct_events

    group by user_session

),

final_sessions as (

    select

        session_id,
        session_date,
        user_id,
        session_start_time,
        session_end_time,
        event_count,
        unique_product_count,
        cart_additions,
        purchase_count,
        view_count,
        total_revenue,
        converted,
        session_length

    from stg_sessions

)

select * from final_sessions
