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
PK: event_date, article_id, device_category
*/
with grouped as (
    select
        event_date
        , article_id
        , device.category as device_category
        , any_value(published_at) as published_at
        , count(1) as page_views
        , count(distinct session_key) as sessions
        , count(distinct user_pseudo_id) as unique_users
        , countif(read_to_end) as read_to_ends
    from
        {{ ref('stg_ga__page_views') }}
    {% if is_incremental() %}
    where event_date >= _dbt_max_partition
    {% endif %}
    group by
        1, 2, 3
)

select
    *
    , event_date = date(published_at) as is_published_date
    , {{ is_1st_week('event_date', 'published_at') }} as is_1st_week
    , {{ is_1st_month('event_date', 'published_at') }} as is_1st_month
from
    grouped