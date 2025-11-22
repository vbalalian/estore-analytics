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

overall as (

    select

        'overall' as metric_level,
        'all_time' as time_period,
        count(*) as total_customers,
        sum(is_churned) as churned_customers,
        round(safe_divide(sum(is_churned), count(*)), 4) as churn_rate

    from customers

),

by_month as (

    select

        'by_first_purchase_month' as metric_level,
        cast(date_trunc(first_purchase_date, month) as string) as time_period,
        count(*) as total_customers,
        sum(is_churned) as churned_customers,
        round(safe_divide(sum(is_churned), count(*)), 4) as churn_rate

    from customers

    group by 2

),

final as (

    select * from overall

    union all

    select * from by_month

)

select * from final
