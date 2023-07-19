select
    team
    , device_category
    , sum(page_views) as page_views
    , sum(sessions) as sessions
    , sum(unique_users) as unique_users
    , sum(read_to_ends) as read_to_ends
    , sum(page_views_1st_week) as page_views_1st_week
    , sum(sessions_1st_week) as sessions_1st_week
    , sum(unique_users_1st_week) as unique_users_1st_week
    , sum(read_to_ends_1st_week) as read_to_ends_1st_week
    , sum(page_views_1st_month) as page_views_1st_month
    , sum(sessions_1st_month) as sessions_1st_month
    , sum(unique_users_1st_month) as unique_users_1st_month
    , sum(read_to_ends_1st_month) as read_to_ends_1st_month
from (
    select
        {{ get_team_name('page_title') }} as team
        , * except(page_title)
    from
        {{ ref('mart_pages__metrics_for_each_device') }}
)
group by 1, 2
order by page_views desc
