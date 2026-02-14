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

cleaned_sessions as (

    select
        user_id,
        count(*) as session_count
    from {{ ref('fct_sessions_cleaned') }}
    group by user_id

),

transformed as (

    select

        user_id,

        min(event_time) as first_event_time,
        min(event_date) as first_event_date,
        max(event_time) as last_event_time,
        max(event_date) as last_event_date,
        count(*) as event_count,

        round(sum(
            case
                when is_purchase = 1
                    then revenue
            end
        ), 2) as total_revenue,

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

        round(avg(
            case
                when is_purchase = 1
                    then revenue
            end
        ), 2) as avg_order_value,

        case
            when
                count(case when is_purchase = 1 then 1 end) > 0
                then greatest(
                    date_diff(
                        max(case when is_purchase = 1 then event_date end),
                        min(case when is_purchase = 1 then event_date end),
                        day
                    ),
                    1
                )
        end as customer_lifespan_days,

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

churn_classified as (

    select
        transformed.user_id,
        transformed.first_event_time,
        transformed.first_event_date,
        transformed.last_event_time,
        transformed.last_event_date,
        coalesce(cleaned_sessions.session_count, 0) as session_count,
        transformed.event_count,
        transformed.total_revenue,
        transformed.purchase_count,
        transformed.first_purchase_date,
        transformed.last_purchase_date,
        transformed.avg_order_value,
        transformed.customer_lifespan_days,
        transformed.days_since_last_activity,
        transformed.days_since_last_purchase,

        transformed.total_revenue as customer_ltv,

        case
            when
                transformed.purchase_count > 0
                and transformed.days_since_last_purchase
                > {{ var('churn_threshold_days') }}
                and transformed.days_since_last_activity
                > {{ var('churn_threshold_days') }}
                then 'churned'
            when
                transformed.purchase_count > 0
                and transformed.days_since_last_purchase
                > {{ var('at_risk_threshold_days') }}
                then 'at_risk'
            when
                transformed.purchase_count > 0
                and transformed.days_since_last_purchase
                > {{ var('declining_threshold_days') }}
                then 'declining'
            when transformed.purchase_count > 0 then 'active'
            else 'prospect'
        end as activity_status,

        case
            when
                transformed.purchase_count > 0
                and transformed.days_since_last_purchase
                > {{ var('churn_threshold_days') }}
                and transformed.days_since_last_activity
                > {{ var('churn_threshold_days') }}
                then 1
            else 0
        end as is_churned,

        transformed.purchase_count > 0 as has_purchase_history,

        case
            when coalesce(cleaned_sessions.session_count, 0) = 0
                then 'missing_sessions'
            when
                cleaned_sessions.session_count > 0
                and safe_divide(
                    transformed.event_count,
                    cleaned_sessions.session_count
                ) > {{ var('session_outlier_ratio') }}
                then 'anomalous_session_ratio'
        end as data_quality_flag

    from transformed

    left join cleaned_sessions
        on transformed.user_id = cleaned_sessions.user_id

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
        customer_ltv,
        activity_status,
        is_churned,
        has_purchase_history,
        data_quality_flag

    from churn_classified

)

select * from final
