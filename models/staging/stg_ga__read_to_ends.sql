/*
read_to_env イベント
*/
select
    *
from
    {{ ref('stg_ga__events') }}
where event_name = 'read_to_end'