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
page_view イベント
*/
with event_extracted as (
    select
        *
    from
        {{ ref('stg_ga__events') }}
    where event_name = 'page_view'
    {% if is_incremental() %}
      and event_date >= _dbt_max_partition
    {% endif %}
),

/*
読了フラグを追加
*/
add_cols as (
    select
        *
    -- 読了フラグ
    , exists (
        select
            page_view_id
        from
            {{ ref('stg_ga__read_to_ends' )}} as read_to_end
        where read_to_end.event_date = event_extracted.event_date
        {% if is_incremental() %}
          and event_date >= _dbt_max_partition
        {% endif %}
          and read_to_end.page_view_id = event_extracted.page_view_id
    ) as read_to_end
    from
        event_extracted
)

select * from add_cols