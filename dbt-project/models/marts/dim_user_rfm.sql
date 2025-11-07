{{ config(
    materialized = 'table',
    cluster_by = ['rfm_score','recency_score','frequency_score','monetary_score']
) }}

with

source_dim_users as (

    select * from {{ ref('dim_users') }}

),

ranked_users as (

    select 

        *,
        dense_rank() over (order by purchase_count) as freq_percentile

    from source_dim_users
    where purchase_count > 0

),

rfm_scores as (
    select

        user_id,
        days_since_last_purchase,
        purchase_count,
        round(total_revenue, 2) as total_revenue,

        ntile(5) over (order by days_since_last_purchase desc) as recency_score,

        case
            when freq_percentile < 0.2 then 1
            when freq_percentile < 0.4 then 2
            when freq_percentile < 0.6 then 3
            when freq_percentile < 0.8 then 4
            else 5
        end as frequency_score,

        ntile(5) over (order by total_revenue) as monetary_score

    from ranked_users
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
