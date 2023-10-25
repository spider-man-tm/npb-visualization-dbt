/*
page_titleからチーム名を取得する
*/

{%- macro get_team_name(page_title) -%}
    case
        when {{ page_title }} like '%埼玉%' then '西武'
        when {{ page_title }} like '%北海道%' then '日本ハム'
        when {{ page_title }} like '%千葉%' then 'ロッテ'
        when {{ page_title }} like '%オリックス%' then 'オリックス'
        when {{ page_title }} like '%東北%' then '楽天'
        when {{ page_title }} like '%福岡%' then 'ソフトバンク'
        when {{ page_title }} like '%阪神%' then '阪神'
        when {{ page_title }} like '%中日%' then '中日'
        when {{ page_title }} like '%読売%' then '巨人'
        when {{ page_title }} like '%ヤクルト%' then 'ヤクルト'
        when {{ page_title }} like '%広島%' then '広島'
        when {{ page_title }} like '%DeNA%' then 'DeNA'
        when {{ page_title }} is null then '不明'
        else '共通ページ'
    end
{%- endmacro -%}