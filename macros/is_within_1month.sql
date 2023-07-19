{% macro is_within_1month(event_date, comparison_date) %}

  {{ event_date }} between date_sub(date({{ comparison_date }}), interval 1 month) and date({{ comparison_date }})

{% endmacro %}