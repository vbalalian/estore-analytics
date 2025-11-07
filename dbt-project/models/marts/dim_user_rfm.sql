{{ config(
    materialized = 'table',
    cluster_by = ['rfm_score','recency_score','frequency_score','monetary_score']
) }}

with

source_dim_users as (

    select * from {{ ref('dim_users') }}

),

rfm_scores as (
    select

        user_id,
        days_since_last_purchase,
        purchase_count,
        round(total_revenue, 2) as total_revenue,

        ntile(5) over (order by days_since_last_purchase desc) as recency_score,
        ntile(5) over (order by purchase_count) as frequency_score,
        ntile(5) over (order by total_revenue) as monetary_score

    from source_dim_users
    where purchase_count > 0
),

final_users_rfm as (

    select

        user_id,
        days_since_last_purchase,
        purchase_count,
        total_revenue,
        recency_score,
        frequency_score,
        monetary_score,

        (recency_score + frequency_score + monetary_score) as rfm_score

    from rfm_scores

)

select * from final_users_rfm
