{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    cluster_by = ['user_id'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

with

source as (

    select * from {{ ref('stg_events') }}

),

max_event_date as (

    select

        max(event_date) as max_date

    from source
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

    {% if is_incremental() %}

        where event_date >= (select date_sub(date(max_date), interval 2 day) from max_event_date)

    {% endif %}

    group by user_id

)

select * from final
