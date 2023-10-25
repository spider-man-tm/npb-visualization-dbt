select
    event_date_dt
    , {{ get_team_name('page_title') }} as team
    , {{ get_player_name('page_title') }} as player
    , sum(page_views) pv
from
    {{ ref('count_page_views_daily__base') }}
group by
    1, 2, 3
order by
    event_date_dt desc, pv desc
