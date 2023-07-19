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


with joined as (
    select
        event_date
        , page_title
        , s.source
        , s.medium
        , session_key
        , user_pseudo_id
        , read_to_end
    from
        {{ ref('stg_ga__page_views') }} pv
        inner join
            {{ ref('stg_ga__sessions') }} s
            using (event_date, user_pseudo_id, session_key)
    {% if is_incremental() %}
    where event_date >= _dbt_max_partition
    {% endif %}
),

/*
PK: event_date, page_title, classify_referrer
*/
grouped as (
    select
        event_date
        , page_title
        , {{ classify_referrer('source', 'medium') }} as referrer
        , count(1) as page_views
        , count(distinct session_key) as sessions
        , count(distinct user_pseudo_id) as unique_users
        , countif(read_to_end) as read_to_ends
    from joined
    group by 1, 2, 3
)

select
    *
    , {{ is_within_1week('event_date', 'current_date()') }} as is_within_1week
    , {{ is_within_1month('event_date', 'current_date()') }} as is_within_1month
from
    grouped
