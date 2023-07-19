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
PK: event_date, page_title, device_category
*/
with grouped as (
    select
        event_date
        , page_title
        , device.category as device_category
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
    , {{ is_within_1week('event_date', 'current_date()') }} as is_within_1week
    , {{ is_within_1month('event_date', 'current_date()') }} as is_within_1month
from
    grouped
