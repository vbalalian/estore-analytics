{{ config(
    materialized='table'
) }}

with

users as (

    select * from {{ ref('dim_users') }}

),

rfm as (

    select * from {{ ref('dim_user_rfm') }}

),

combined as (

    select

        users.user_id,
        users.session_count,
        users.event_count,
        users.total_revenue,
        users.purchase_count,
        users.avg_order_value,
        users.customer_lifespan_days,
        users.customer_ltv,
        users.is_churned,
        rfm.rfm_score,
        rfm.rfm_segment

    from users

    inner join rfm
        on users.user_id = rfm.user_id

),

segment_summary as (

    select

        rfm_segment,
        count(*) as customer_count,

        round(avg(session_count), 2) as avg_session_count,
        round(avg(event_count), 2) as avg_event_count,
        round(avg(purchase_count), 2) as avg_purchase_count,
        round(avg(avg_order_value), 2) as avg_order_value,
        round(avg(customer_lifespan_days), 2) as avg_lifespan_days,
        round(avg(customer_ltv), 2) as avg_customer_ltv,
        sum(is_churned) as total_churned,
        round(sum(is_churned) / count(*), 2) as churn_rate,

        round(avg(rfm_score), 2) as avg_rfm_total,
        approx_quantiles(rfm_score, 100)[offset(50)] as med_rfm_total,

        round(avg(total_revenue), 2) as avg_revenue,
        approx_quantiles(total_revenue, 100)[offset(50)] as med_revenue,
        round(stddev(total_revenue), 2) as std_dev_revenue,
        approx_quantiles(total_revenue, 100)[offset(25)] as p25_revenue,
        approx_quantiles(total_revenue, 100)[offset(75)] as p75_revenue

    from combined

    group by rfm_segment

),

segments_w_context as (

    select

        rfm_segment,
        customer_count,
        avg_session_count,
        avg_event_count,
        avg_purchase_count,
        avg_order_value,
        avg_lifespan_days,
        avg_customer_ltv,
        total_churned,
        churn_rate,
        avg_rfm_total,
        med_rfm_total,
        avg_revenue,
        med_revenue,
        std_dev_revenue,
        p25_revenue,
        p75_revenue,

        case rfm_segment
            when 'Champions' then 'Best Customers'
            when 'Loyal Customers' then 'Retention Focus'
            when 'Potential Loyalists' then 'Growth Opportunity'
            when 'Recent Customers' then 'Onboarding'
            when 'Promising' then 'Develop'
            when 'Needs Attention' then 'Re-Engage'
            when 'About To Sleep' then 'Win Back'
            when 'At Risk' then 'Recover'
            when 'Lost' then 'Inactive'
            else 'Other'
        end as category_type,

        case rfm_segment
            when
                'Champions'
                then 'Reward loyalty, early access offers, VIP programs'
            when
                'Loyal Customers'
                then 'Upsell, cross-sell, maintain engagement'
            when 'Potential Loyalists' then 'Encourage second/third purchase'
            when 'Recent Customers' then 'Welcome sequence, nurture campaigns'
            when 'Promising' then 'Promote value reminders, engagement nudges'
            when 'Needs Attention' then 'Send personalized offers or check-ins'
            when 'About To Sleep' then 'Send reactivation campaigns soon'
            when 'At Risk' then 'Strong win-back offer or feedback survey'
            when 'Lost' then 'Occasional reactivation attempt or ignore'
            else 'Unclassified strategy'
        end as action_strategy

    from segment_summary

),

final as (

    select

        rfm_segment,
        customer_count,
        avg_session_count,
        avg_event_count,
        avg_purchase_count,
        avg_order_value,
        avg_lifespan_days,
        avg_customer_ltv,
        total_churned,
        churn_rate,
        avg_rfm_total,
        avg_revenue,
        med_rfm_total,
        med_revenue,
        std_dev_revenue,
        p25_revenue,
        p75_revenue,
        category_type,
        action_strategy

    from segments_w_context
)

select * from final
