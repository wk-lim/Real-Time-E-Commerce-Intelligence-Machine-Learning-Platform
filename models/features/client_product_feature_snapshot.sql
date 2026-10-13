select
    c.feature_as_of_date_source,
    c.client_id,
    c.sku,
    a.events_previous_day,
    a.page_visits_previous_day,
    a.search_previous_day,
    a.distinct_skus_previous_day,
    a.cart_additions_previous_day as client_cart_additions_previous_day,
    a.cart_removals_previous_day as client_cart_removals_previous_day,
    a.purchases_previous_day as client_purchases_previous_day,
    c.sku_interactions_previous_day,
    c.cart_additions_previous_day as sku_cart_additions_previous_day,
    c.cart_removals_previous_day as sku_cart_removals_previous_day,
    c.purchases_previous_day as sku_purchases_previous_day
from {{ ref('client_product_candidates') }} as c
left join {{ ref('client_prior_day_activity') }} as a
    on c.client_id = a.client_id
    and c.feature_as_of_date_source = a.feature_as_of_date_source