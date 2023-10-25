select
    event_date_dt
    , {{ get_team_name('page_title') }} as team
    , sum(page_views) pv
from
    {{ ref('count_page_views_daily__base') }}
group by
    1, 2
order by
    event_date_dt desc, pv desc
