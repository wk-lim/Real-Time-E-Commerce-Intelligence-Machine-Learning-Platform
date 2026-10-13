with daily_activity as (
    select
        client_id,
        cast(event_time_local as date) as activity_date_source,
        count(*) as events_previous_day,
        count(*) filter (where event_type = 'page_visit') as page_visits_previous_day,
        count(*) filter (where event_type = 'search_query') as search_previous_day,
        count(*) filter (where event_type = 'add_to_cart') as cart_additions_previous_day,
        count(*) filter (where event_type = 'remove_from_cart') as cart_removals_previous_day,
        count(*) filter (where event_type = 'product_buy') as purchases_previous_day,
        count(distinct sku) as distinct_skus_previous_day
    from {{ ref('stg_events') }}
    group by 1, 2
)

select
    client_id,
    activity_date_source + 1 as feature_as_of_date_source,
    events_previous_day,
    page_visits_previous_day,
    search_previous_day,
    cart_additions_previous_day,
    cart_removals_previous_day,
    purchases_previous_day,
    distinct_skus_previous_day
from daily_activity