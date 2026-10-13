select
    client_id,
    sku,
    feature_as_of_date_source,
    count(*) as rows_at_grain
from {{ ref('client_product_candidates') }}
group by 1, 2, 3
having count(*) > 1
    or client_id is null
    or sku is null
    or feature_as_of_date_source is null