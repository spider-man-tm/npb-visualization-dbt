select
    event_date_dt
    , page_title
    , user_source
    , user_medium
    , device_category
    , device_mobile_brand_name
    , device_web_info_browser
    , geo_country
    , geo_region
    , count(*) as page_views
    , count(distinct session_key) as sessions
    , count(distinct user_pseudo_id) as unique_users
from
    {{ ref('marts_core__fct_page_view') }}
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9
