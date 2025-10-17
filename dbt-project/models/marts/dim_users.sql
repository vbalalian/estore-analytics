{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    cluster_by = ['user_id'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

{% if is_incremental() %}
    max_event_date as (

        select max(event_date) as max_date

        from {{ ref('stg_events') }}
    ),
{% endif %}

source as (

    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}

        where
            event_date
            >= (
                select date_sub(date(max_event_date.max_date), interval 2 day)
                from max_event_date
            )

    {% endif %}

),

final as (

    select

        user_id,
        min(event_time) as first_event_time,
        min(event_date) as first_event_date,
        max(event_time) as last_event_time,
        max(event_date) as last_event_date,
        count(distinct user_session) as session_count,
        count(*) as event_count

    from source

    group by user_id

)

select * from final
