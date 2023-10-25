select
    event_date_dt
    , {{ classify_region('geo_country', 'geo_region') }} region
    , sum(page_views) pv
from
    {{ ref('count_page_views_daily__base') }}
group by
    1, 2
order by
    event_date_dt desc, pv desc