{{ 
    config(materialized = 'table')
}}

with

users as (

    select * from {{ ref('dim_users') }}

),

customers as (

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
        is_churned

    from users

    where purchase_count > 0

),

dataset_range as (

    select

        min(first_purchase_date) as dataset_start,
        max(first_purchase_date) as dataset_end,
        date_diff(max(first_purchase_date), min(first_purchase_date), day)
            as dataset_days

    from customers

),

cohort_eligible_customers as (

    select

        customers.user_id,
        customers.first_event_time,
        customers.first_event_date,
        customers.last_event_time,
        customers.last_event_date,
        customers.session_count,
        customers.event_count,
        customers.total_revenue,
        customers.purchase_count,
        customers.first_purchase_date,
        customers.last_purchase_date,
        customers.avg_order_value,
        customers.customer_lifespan_days,
        customers.days_since_last_activity,
        customers.days_since_last_purchase,
        customers.customer_ltv,
        customers.activity_status,
        customers.is_churned,

        dataset_range.dataset_end,
        date_diff(dataset_range.dataset_end, customers.first_purchase_date, day)
            as days_observable

    from customers, dataset_range

    where
        date_diff(
            dataset_range.dataset_end, customers.first_purchase_date, day
        ) >= {{ var('cohort_observation_window_days') }}

),

churn_flags as (

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
        dataset_end,
        days_observable,

        case
            when purchase_count = 1 then 1
            when
                date_diff(last_purchase_date, first_purchase_date, day)
                > {{ var('cohort_observation_window_days') }}
                then 0
            else 1
        end as is_churned_cohort

    from cohort_eligible_customers

),

overall as (

    select

        'cohort_overall' as metric_level,
        'all_eligible_cohorts' as time_period,
        count(*) as total_customers,
        sum(is_churned_cohort) as churned_customers,
        round(safe_divide(sum(is_churned_cohort), count(*)), 4) as churn_rate,
        min(first_purchase_date) as cohort_start,
        max(first_purchase_date) as cohort_end

    from churn_flags

),

by_cohort_month as (

    select

        'by_cohort_month' as metric_level,
        cast(date_trunc(first_purchase_date, month) as string) as time_period,
        count(*) as total_customers,
        sum(is_churned_cohort) as churned_customers,
        round(safe_divide(sum(is_churned_cohort), count(*)), 4) as churn_rate,
        min(first_purchase_date) as cohort_start,
        max(first_purchase_date) as cohort_end

    from churn_flags

    group by time_period

),

final as (

    select * from overall
    union all
    select * from by_cohort_month

)

select * from final
