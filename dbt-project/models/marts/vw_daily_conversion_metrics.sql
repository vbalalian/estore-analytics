{{ config(materialized='view') }}

with

conversion_metrics as (

    select * from {{ ref('metrics_conversion_rates') }}

),

daily_conversion_metrics as (

    select

        total_sessions,
        sessions_with_view,
        sessions_with_cart,
        sessions_with_purchase,
        view_to_cart_rate,
        cart_to_purchase_rate,
        view_to_purchase_rate,
        avg_order_value,
        date(time_period) as metric_date

    from conversion_metrics

    where metric_level = 'daily'

),

final as (

    select

        metric_date,
        total_sessions,
        sessions_with_view,
        sessions_with_cart,
        sessions_with_purchase,
        view_to_cart_rate,
        cart_to_purchase_rate,
        view_to_purchase_rate,
        avg_order_value

    from daily_conversion_metrics

)

select * from final
