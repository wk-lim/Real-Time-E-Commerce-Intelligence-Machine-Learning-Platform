with staged as (
    select count(*) as event_count
    from {{ ref('stg_events') }}
),
mart as (
    select coalesce(sum(event_count), 0) as event_count
    from {{ ref('fct_daily_event_activity') }}
)

select
    staged.event_count as staged_event_count,
    mart.event_count as mart_event_count
from staged
cross join mart
where staged.event_count <> mart.event_count