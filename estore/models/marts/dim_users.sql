with final as (

select
    user_id,
    min(event_time) as first_event_time,
    min(event_date) as first_event_date,
    max(event_time) as last_event_time,
    max(event_date) as last_event_date,
    count(distinct user_session) as session_count,
    count(*) as event_count
from {{ ref('stg_events') }}
group by user_id

)

select * from final
