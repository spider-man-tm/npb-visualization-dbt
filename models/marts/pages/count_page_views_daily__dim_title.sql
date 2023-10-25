select
    event_date_dt
    , page_title
    , sum(page_views) as pv
from
    {{ ref('count_page_views_daily__base') }}
group by
    1, 2
order by
    event_date_dt desc, pv desc
