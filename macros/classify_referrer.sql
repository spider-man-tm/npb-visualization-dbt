{%- macro classify_referrer(user_source, user_medium) -%}

    case
        when {{ user_medium }} = 'organic' then '検索エンジン'
        when {{ user_source }} like '%google%' then 'google'
        when {{ user_source }} = '(direct)' or {{ user_source }} is null then '不明'
        when {{ user_source }} in ('twitter.com', 't.co') then 'twitter'
        when {{ user_source }} like '%yahoo%' then 'yahoo'
        when {{ user_source }} like '%facebook%' then 'facebook'
        when {{ user_source }} like '%hatena%' then 'はてな'
        when {{ user_source }} like 'linkedin.com' then 'linkedin'
        when {{ user_source }} like 'github.com' then 'github'
        when {{ user_source }} = 'baidu' then 'baidu'
        else 'その他'
    end

{%- endmacro -%}