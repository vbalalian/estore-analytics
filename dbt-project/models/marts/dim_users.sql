{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    cluster_by = ['user_id'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

source as (

    select * from {{ ref('fct_events') }}

),

max_date as (

    select max(event_date) as max_date
    from source

),

transformed as (

    select

        user_id,
        min(event_time) as first_event_time,
        min(event_date) as first_event_date,
        max(event_time) as last_event_time,
        max(event_date) as last_event_date,
        count(distinct user_session) as session_count,
        count(*) as event_count,

        sum(
            case
                when is_purchase = 1
                    then revenue
            end
        ) as total_revenue,
        count(
            case
                when is_purchase = 1
                    then 1
            end
        ) as purchase_count,
        min(
            case
                when is_purchase = 1
                    then event_date
            end
        ) as first_purchase_date,
        max(
            case
                when is_purchase = 1
                    then event_date
            end
        ) as last_purchase_date,
        avg(
            case
                when is_purchase = 1
                    then revenue
            end
        ) as avg_order_value,
        date_diff(
            max(case when is_purchase = 1 then event_date end),
            min(case when is_purchase = 1 then event_date end),
            day
        ) as customer_lifespan_days,
        date_diff(
            (select max_date.max_date from max_date),
            max(event_date),
            day
        ) as days_since_last_activity,
        date_diff(
            (select max_date.max_date from max_date),
            max(case when is_purchase = 1 then event_date end),
            day
        ) as days_since_last_purchase

    from source

    group by user_id

),

final as (

    select
        user_id,
        first_event_time,
        first_event_date,
        last_event_time,
        last_event_date,
        session_count,
        event_count,
        total_revenue,
        purchase_count,
        first_purchase_date,
        last_purchase_date,
        avg_order_value,
        customer_lifespan_days,
        days_since_last_activity,
        days_since_last_purchase,

        total_revenue as customer_ltv,

        case
            when
                purchase_count > 0 and days_since_last_purchase > 120
                then 'churned'
            when
                purchase_count > 0 and days_since_last_purchase > 90
                then 'at_risk'
            when
                purchase_count > 0 and days_since_last_purchase > 60
                then 'declining'
            when purchase_count > 0 then 'active'
            else 'prospect'
        end as activity_status,

        case
            when purchase_count > 0 and days_since_last_purchase > 120 then 1
            else 0
        end as is_churned

    from transformed

)

select * from final
