select
    cast(event_time_local as date) as event_date_source,
    event_type,
    count(*) as event_count,
    count(distinct client_id) as distinct_clients
from {{ ref('stg_events') }}
group by 1, 2