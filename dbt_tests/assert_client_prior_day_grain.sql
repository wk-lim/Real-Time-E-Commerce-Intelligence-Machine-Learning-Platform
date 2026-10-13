select
    client_id,
    feature_as_of_date_source,
    count(*) as row_count
from {{ ref('client_prior_day_activity') }}
group by 1, 2
having count(*) > 1