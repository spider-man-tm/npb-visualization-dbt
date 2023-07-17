{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'event_date',
            'data_type': 'date'
        }
    )
}}


/*
PK: event_date
*/
select
    event_date
    , count(1) as unique_users
    , countif(frequency_segment = 'light') as light_users
    , countif(frequency_segment = 'medium') as medium_users
    , countif(frequency_segment = 'heavy') as heavy_users
    , countif(frequency_segment = 'royal') as royal_users
    , sum(sessions) as sessions
    , sum(page_views) as page_views
    , sum(articles) as articles
    , sum(read_to_ends) as read_to_ends
from
    {{ ref('stg_ga__users') }}
where is_visited_on_the_day
{% if is_incremental() %}
    and event_date >= _dbt_max_partition
{% endif %}
group by
    event_date