/*
click イベント
*/
with event_extracted as (
    select
        *
    from
        {{ ref('stg_ga__events') }}
    where event_name = 'click'
),

/*
上記テーブルを正規化
*/
unnested as (
    select
        *
        , {{ ga_unnest_key('event_params', 'track_category') }}
        , {{ ga_unnest_key('event_params', 'link_url') }}
        , {{ ga_unnest_key('event_params', 'outbound') }}
    from
        event_extracted
),

/*
URLを正規化
*/
casted as (
    select
        * except (outbound)
        , {{ normalize_url('link_url') }} link_url_canonical
        , coalesce(cast(outbound as boolean), false) as outbound
    from
        unnested
)

select * from casted