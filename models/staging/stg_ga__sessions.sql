{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'event_date',
            'data_type': 'date'
        },
        tags = ['hourly']
    )
}}


/*
session イベント
PK: session_key, user_pseudo_id
*/
with session_agg as (
    select
        session_key
        , user_pseudo_id
        , min(event_date) as event_date
        , count(1) as page_views
        , count(distinct article_id) as articles
        , count(distinct if(read_to_end, article_id, null)) as read_to_ends
    from
        {{ ref('stg_ga__page_views') }}
    {% if is_incremental() %}
    where event_date >= _dbt_max_partition
    {% endif %}
    group by
        session_key, user_pseudo_id
),

/*
直前のsession 情報
PK: session_key
*/
session_lag as (
    select distinct
        session_key
        , created_at
        , article_id
        , page_url
        , source
        , medium
        , campaign
        , content
        , term
        , ga_session_number
        , user_id
        , lag(user_pseudo_id) over (partition by session_key order by event_timestamp) as lag_session_key
        , lead(user_pseudo_id) over (partition by session_key order by event_timestamp) as lead_session_key
    from
        {{ ref('stg_ga__page_views') }}
    {% if is_incremental() %}
    where event_date >= _dbt_max_partition
    {% endif %}
),

/*
first session
PK: session_key
*/
session_entrance as (
    select
        session_key
        , created_at as entrance_timestamp
        , article_id as entrance_article_id
        , page_url as entrance_page_url
        , source
        , medium
        , campaign
        , content
        , term
        , ga_session_number
    from
        session_lag
    where lag_session_key is null
),

/*
last session
PK: session_key
*/
session_exit AS (
    select
        session_key
        , created_at as exit_timestamp
        , article_id as exit_article_id
        , page_url as exit_page_url
        , user_id
    from
        session_lag
    where lead_session_key is null
)

select
    *
    , timestamp_diff(exit_timestamp, entrance_timestamp, second) as session_duration
from
    session_agg
    inner join session_entrance
        using (session_key)
    inner join session_exit
        using (session_key)