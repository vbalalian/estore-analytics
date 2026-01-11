{{ config(
    materialized='table'
) }}

with source_sessions as (
    select * from {{ ref('fct_sessions') }}
),

overall_metrics as (
    select
        'overall' as metric_level,
        'all_time' as time_period,

        count(distinct session_id) as total_sessions,
        sum(reached_view) as sessions_with_view,
        sum(reached_cart) as sessions_with_cart,
        sum(reached_purchase) as sessions_with_purchase,

        {{ rate_metric('sum(reached_cart)', 'sum(reached_view)') }}
            as view_to_cart_rate,
        {{ rate_metric('sum(reached_purchase)', 'sum(reached_cart)') }}
            as cart_to_purchase_rate,
        {{ rate_metric('sum(reached_purchase)', 'sum(reached_view)') }}
            as view_to_purchase_rate,

        round(sum(total_revenue), 2) as total_revenue,
        round(avg(case when reached_purchase = 1 then total_revenue end), 2)
            as avg_order_value

    from source_sessions
),

daily_metrics as (
    select
        'daily' as metric_level,
        cast(session_start_date as string) as time_period,

        count(distinct session_id) as total_sessions,
        sum(reached_view) as sessions_with_view,
        sum(reached_cart) as sessions_with_cart,
        sum(reached_purchase) as sessions_with_purchase,

        {{ rate_metric('sum(reached_cart)', 'sum(reached_view)') }}
            as view_to_cart_rate,
        {{ rate_metric('sum(reached_purchase)', 'sum(reached_cart)') }}
            as cart_to_purchase_rate,
        {{ rate_metric('sum(reached_purchase)', 'sum(reached_view)') }}
            as view_to_purchase_rate,

        round(sum(total_revenue), 2) as total_revenue,
        round(avg(case when reached_purchase = 1 then total_revenue end), 2)
            as avg_order_value

    from source_sessions
    group by session_start_date
),

final as (
    select * from overall_metrics
    union all
    select * from daily_metrics
)

select * from final
