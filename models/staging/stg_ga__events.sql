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
eventsテーブル
*/
with base as (
    select
        parse_date('%Y%m%d', event_date) event_date
        , event_timestamp
        , event_name
        , event_params
        , event_previous_timestamp
        , event_bundle_sequence_id
        , event_server_timestamp_offset
        , user_id
        , user_pseudo_id
        , privacy_info
        , user_properties
        , user_first_touch_timestamp
        , device
        , geo
        , app_info
        , traffic_source
        , stream_id
        , platform
    from
        {{ source('ga', 'events') }}
    -- GA4とBigQueryの連携で日次だけでなくリアルタイム連携を選択していると、intradayテーブルもできてしまっている
    where _TABLE_SUFFIX not like '%intraday%'
        -- 指定したsufix以降のテーブルのみ対象
        and cast(_TABLE_SUFFIX as int64) >= {{ var('ga_start_date') }}
    -- 増分データのみ
    {% if is_incremental() %}
        and parse_date('%Y%m%d', _TABLE_SUFFIX) >= _dbt_max_partition
    {% endif %}
),

/*
enevtsテーブルを正規化
*/
unnested as (
    select
        *
        , {{ ga_unnest_key('event_params', 'ga_session_id', 'int_value') }}
        , {{ ga_unnest_key('event_params', 'ga_session_number',  'int_value') }}
        , {{ ga_unnest_key('event_params', 'page_location', rename_column = 'page_url') }}
        , {{ ga_unnest_key('event_params', 'page_title') }}
        , {{ ga_unnest_key('event_params', 'page_referrer') }}
        , {{ ga_unnest_key('event_params', 'source') }}
        , {{ ga_unnest_key('event_params', 'medium') }}
        , {{ ga_unnest_key('event_params', 'campaign') }}
        , {{ ga_unnest_key('event_params', 'content') }}
        , {{ ga_unnest_key('event_params', 'term') }}
        , {{ ga_unnest_key('event_params', 'page_view_id') }}
        , {{ ga_unnest_key('event_params', 'page_type') }}
        , {{ ga_unnest_key('event_params', 'article_id', if_null = 'check_int_value') }}
        , {{ ga_unnest_key('event_params', 'article_type') }}
        , {{ ga_unnest_key('event_params', 'published_at') }}
    from base
),

/*
クリーニング
*/
cleaned as (
    select
        * except (event_name, published_at)
        , lower(replace(trim(event_name), '', '_')) event_name
        , SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', published_at) published_at
    from unnested
),

add_cols as (
    select
        *
        , {{ normalize_url('page_url') }} as page_url_canonical
        , TO_BASE64(MD5(concat(stream_id, user_pseudo_id, cast(ga_session_id as string)))) as session_key
        , {{ convert_region('geo.region') }} as prefecture
        , TIMESTAMP_MICROS(event_timestamp) as created_at
        , {{ calc_page_number('page_url', 'page_type', 'device.category') }} as page_number
    from cleaned
)

select * from add_cols