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


with target_term as (
    select
        event_occured_date
        , target_date
    from
    {% if is_incremental() %}
        unnest(generate_date_array(date_sub(_dbt_max_partition, interval 27 day), current_date('Asia/Tokyo'))) as event_occured_date
        , unnest(generate_date_array(_dbt_max_partition, current_date('Asia/Tokyo'))) as target_date
    {% else %}
        unnest(generate_date_array(parse_date('%Y%m%d', cast({{ var('ga_start_date') }} as string)), current_date('Asia/Tokyo'))) as event_occured_date
        , unnest(generate_date_array(event_occured_date, date_add(event_occured_date, interval 27 day))) as target_date
    {% endif %}
),

/*
PK: user_pseudo_id, event_occured_date
*/
user_performance as (
    select
        user_pseudo_id
        , event_date as event_occured_date
        , max(user_id) as user_id
        , count(1) as sessions
        , sum(page_views) as page_views
        , sum(articles) as articles
        , sum(read_to_ends) as read_to_ends
    from
        {{ ref('stg_ga__sessions') }}
    {% if is_incremental() %}
    where event_date >= date_sub(_dbt_max_partition, interval 27 day)
    {% endif %}
    group by
        1, 2
),

/*
user_performanceをtarget_termごとに集計
PK: user_pseudo_id, target_date
*/
joined as (
    select
        user_pseudo_id
        , target_date as event_date
        , max(user_id) as user_id
        , count(distinct event_occured_date) as frequency
        , count(1) as sessions_last_4_weeks
        , sum(page_views) as page_views_last_4_weeks
        , sum(articles) as articles_last_4_weeks
        , sum(read_to_ends) as read_to_ends_last_4_weeks
        , logical_or(event_occured_date = target_date) as is_visited_on_the_day
        , countif(event_occured_date = target_date) as sessions
        , sum(if(event_occured_date = target_date, page_views, 0)) as page_views
        , sum(if(event_occured_date = target_date, articles, 0)) as articles
        , sum(if(event_occured_date = target_date, read_to_ends, 0)) as read_to_ends
    from
        target_term
        inner join
            user_performance using (event_occured_date)
    where target_date <= current_date('Asia/Tokyo')
    group by
        1, 2
)


select
    *
    , {{ calc_frequency_segment('frequency') }} as frequency_segment
from
    joined