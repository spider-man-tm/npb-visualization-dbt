{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'event_week',
            'data_type': 'date'
        }
    )
}}


with stg_user_metrics as (
    select
        * except (event_date)
        , date_trunc(event_date, week(monday)) as event_week
    from
        {{ ref('stg_ga__users') }}
    {% if is_incremental() %}
    where
        event_date >= _dbt_max_partition
    qualify
        row_number() over (partition by date_trunc(event_date, week(monday)) order by event_date desc) = 1
    {% endif %}
),

user_metrics as (
    select
        event_week
        , count(1) as unique_users
        , countif(frequency_segment = 'light') as light_users
        , countif(frequency_segment = 'medium') as medium_users
        , countif(frequency_segment = 'heavy') as heavy_users
        , countif(frequency_segment = 'royal') as royal_users
        , sum(sessions_last_4_weeks) as sessions
        , sum(if(frequency_segment = 'light', sessions_last_4_weeks, 0)) as light_users_sessions
        , sum(if(frequency_segment = 'medium', sessions_last_4_weeks, 0)) as medium_users_sessions
        , sum(if(frequency_segment = 'heavy', sessions_last_4_weeks, 0)) as heavy_users_sessions
        , sum(if(frequency_segment = 'royal', sessions_last_4_weeks, 0)) as royal_users_sessions
        , sum(page_views_last_4_weeks) as page_views
        , sum(if(frequency_segment = 'light', page_views_last_4_weeks, 0)) as light_users_page_views
        , sum(if(frequency_segment = 'medium', page_views_last_4_weeks, 0)) as medium_users_page_views
        , sum(if(frequency_segment = 'heavy', page_views_last_4_weeks, 0)) as heavy_users_page_views
        , sum(if(frequency_segment = 'royal', page_views_last_4_weeks, 0)) as royal_users_page_views
        , sum(read_to_ends_last_4_weeks) as read_to_ends
        , sum(if(frequency_segment = 'light', read_to_ends_last_4_weeks, 0)) as light_users_read_to_ends
        , sum(if(frequency_segment = 'medium', read_to_ends_last_4_weeks, 0)) as medium_users_read_to_ends
        , sum(if(frequency_segment = 'heavy', read_to_ends_last_4_weeks, 0)) as heavy_users_read_to_ends
        , sum(if(frequency_segment = 'royal', read_to_ends_last_4_weeks, 0)) as royal_users_read_to_ends
        , sum(articles_last_4_weeks) as articles
        , sum(if(frequency_segment = 'light', articles_last_4_weeks, 0)) as light_users_articles
        , sum(if(frequency_segment = 'medium', articles_last_4_weeks, 0)) as medium_users_articles
        , sum(if(frequency_segment = 'heavy', articles_last_4_weeks, 0)) as heavy_users_articles
        , sum(if(frequency_segment = 'royal', articles_last_4_weeks, 0)) as royal_users_articles
    from
        stg_user_metrics
    group by
        1
)


select
  *
  , safe_divide(sessions,unique_users) as frequency
  , safe_divide(page_views, sessions) as pages_per_session
  , safe_divide(read_to_ends, articles) as read_to_end_rate
from
  user_metrics