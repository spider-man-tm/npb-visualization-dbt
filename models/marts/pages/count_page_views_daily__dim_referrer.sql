select
    event_date_dt
    , {{ classify_referrer('user_source', 'user_medium') }} as referrer
    , sum(page_views) pv
from
    {{ ref('count_page_views_daily__base') }}
group by
    1, 2
order by
    event_date_dt desc, pv desc
