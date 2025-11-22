{{ config(
    materialized='table'
) }}

with

source_dim_user_rfm as (

    select * from {{ ref('dim_user_rfm') }}

),

segment_summary as (

    select

        rfm_segment,
        count(*) as customer_count,
        round(avg(rfm_score), 2) as avg_rfm_total,
        round(avg(total_revenue), 2) as avg_revenue,
        approx_quantiles(rfm_score, 100)[offset(50)] as med_rfm_total,
        approx_quantiles(total_revenue, 100)[offset(50)] as med_revenue,
        round(stddev(total_revenue), 2) as std_dev_revenue,
        approx_quantiles(total_revenue, 100)[offset(25)] as p25_revenue,
        approx_quantiles(total_revenue, 100)[offset(75)] as p75_revenue

    from source_dim_user_rfm

    group by rfm_segment

),

segments_w_context as (

    select

        rfm_segment,
        customer_count,
        avg_rfm_total,
        avg_revenue,
        med_rfm_total,
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
