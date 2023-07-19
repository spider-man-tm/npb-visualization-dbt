select
    page_title
    , referrer
    , sum(page_views) as page_views
    , sum(sessions) as sessions
    , sum(unique_users) as unique_users
    , sum(read_to_ends) as read_to_ends
    , sum(if(is_within_1week, page_views, 0)) as page_views_1st_week
    , sum(if(is_within_1week, sessions, 0)) as sessions_1st_week
    , sum(if(is_within_1week, unique_users, 0)) as unique_users_1st_week
    , sum(if(is_within_1week, read_to_ends, 0)) as read_to_ends_1st_week
    , sum(if(is_within_1month, page_views, 0)) as page_views_1st_month
    , sum(if(is_within_1month, sessions, 0)) as sessions_1st_month
    , sum(if(is_within_1month, unique_users, 0)) as unique_users_1st_month
    , sum(if(is_within_1month, read_to_ends, 0)) as read_to_ends_1st_month
from
    {{ ref('mart_pages__daily_metrics_for_each_referrer') }}
group by
    1, 2