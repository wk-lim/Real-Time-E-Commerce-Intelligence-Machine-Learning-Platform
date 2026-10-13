with staged as (
    select count(*) as event_count
    from {{ ref('stg_events') }}
),
features as (
    select coalesce(sum(events_previous_day), 0) as event_count
    from {{ ref('client_prior_day_activity') }}
)

select
    staged.event_count as staged_events,
    features.event_count as feature_events
from staged
cross join features
where staged.event_count <> features.event_count