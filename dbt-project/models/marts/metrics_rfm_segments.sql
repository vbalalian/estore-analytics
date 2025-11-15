{{ config(
    materialized='table'
) }}

with

source_dim_user_rfm as (

    select * from {{ ref('dim_user_rfm') }}

),

rfm_labeled as (
    select
        user_id,
        total_revenue,
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score + frequency_score + monetary_score) as rfm_total,
        case
            when
                recency_score >= 4
                and frequency_score >= 4
                and monetary_score >= 4
                then 'Champions'
            when
                recency_score <= 2
                and frequency_score >= 4
                and monetary_score >= 4
                then 'At Risk'
            when
                recency_score >= 4 and frequency_score >= 3
                then 'Loyal Customers'
            when
                recency_score >= 3 and frequency_score >= 3
                then 'Potential Loyalists'
            when
                recency_score >= 4 and frequency_score < 3
                then 'Recent Customers'
            when recency_score = 3 and frequency_score < 3 then 'Promising'
            when
                recency_score <= 2 and frequency_score >= 3
                then 'About To Sleep'
            when
                recency_score <= 2
                and frequency_score <= 2
                and monetary_score <= 2
                then 'Lost'
            else 'Needs Attention'
        end as rfm_segment
    from source_dim_user_rfm
),

segment_summary as (
    select
        rfm_segment,
        COUNT(*) as customer_count,
        ROUND(AVG(rfm_total), 2) as avg_rfm_total,
        ROUND(AVG(total_revenue), 2) as avg_revenue
    from rfm_labeled
    group by rfm_segment
),

final as (

    select
        rfm_segment,
        customer_count,
        avg_rfm_total,
        avg_revenue,
        case rfm_segment
            when 'Champions' then 1
            when 'Loyal Customers' then 2
            when 'Potential Loyalists' then 3
            when 'Recent Customers' then 4
            when 'Promising' then 5
            when 'Needs Attention' then 6
            when 'About To Sleep' then 7
            when 'At Risk' then 8
            when 'Lost' then 9
            else 10
        end as segment_rank,
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
    order by segment_rank
)

select * from final
