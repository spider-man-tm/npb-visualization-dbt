{% macro is_within_1week(event_date, comparison_date) %}

  {{ event_date }} between date_sub(date({{ comparison_date }}), interval 6 day) and date({{ comparison_date }})

{% endmacro %}