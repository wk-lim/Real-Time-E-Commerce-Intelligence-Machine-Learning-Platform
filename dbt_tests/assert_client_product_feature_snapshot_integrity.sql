with snapshot_keys as (
    select
        client_id,
        sku,
        feature_as_of_date_source,
        count(*) as rows_at_grain,
        min(events_previous_day) as events_previous_day
    from {{ ref('client_product_feature_snapshot') }}
    group by 1, 2, 3
)

select
    coalesce(c.client_id, s.client_id) as client_id,
    coalesce(c.sku, s.sku) as sku,
    coalesce(c.feature_as_of_date_source, s.feature_as_of_date_source)
        as feature_as_of_date_source
from {{ ref('client_product_candidates') }} as c
full outer join snapshot_keys as s
    on c.client_id = s.client_id
   and c.sku = s.sku
   and c.feature_as_of_date_source = s.feature_as_of_date_source
where c.client_id is null
   or s.client_id is null
   or s.rows_at_grain <> 1
   or s.events_previous_day is null