{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {
            'field': 'event_date_dt',
            'data_type': 'date'
        }
    )
}}


select
    event_date_dt
    , page_title
    , page_location
    , page_hostname
    , user_source
    , user_medium
    , device_category
    , device_mobile_brand_name
    , device_web_info_browser
    , geo_country
    , geo_region
    , session_key
    , user_pseudo_id
from
    {{ ref('stg_ga4__event_page_view') }}

{% if is_incremental() %}
    where event_date_dt >= _dbt_max_partition
{% endif %}
