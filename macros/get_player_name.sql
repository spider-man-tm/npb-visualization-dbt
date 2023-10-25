/*
page_titleから選手名を取得する
*/

{%- macro get_player_name(page_title) -%}
    case 
        when instr(page_title, '-') > instr(page_title, '】') then
        trim(substr(
            page_title,
            instr(page_title, '】') + 1,
            instr(page_title, '-') - instr(page_title, '】') - 1
        ))
        else NULL
    end 
{%- endmacro -%}