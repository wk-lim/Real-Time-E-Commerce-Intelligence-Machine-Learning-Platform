select
    client_id,
    sku,
    cast(event_time_local as date) + 1 as feature_as_of_date_source,
    count(*) as sku_interactions_previous_day,
    count(*) filter (where event_type = 'add_to_cart') as cart_additions_previous_day,
    count(*) filter (where event_type = 'remove_from_cart') as cart_removals_previous_day,
    count(*) filter (where event_type = 'product_buy') as purchases_previous_day,
    max(event_time_local) as last_sku_interaction_at_source
from {{ ref('stg_events') }}
where sku is not null
    and event_type in ('add_to_cart', 'remove_from_cart', 'product_buy')
group by 1, 2, 3