select
    event_date_source,
    event_type,
    count(*) as row_count
from {{ ref('fct_daily_event_activity') }}
group by 1, 2
having count(*) > 1