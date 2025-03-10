{% macro calculate_time_window_aggs(relation, group_by_cols, timestamp_col, metric_cols, time_windows) %}

{# Default time windows if not specified #}
{% set default_windows = [7, 30, 90] %}
{% set windows = time_windows if time_windows is defined else default_windows %}

with source_data as (
    select * from {{ relation }}
),

{% for window in windows %}
window_{{ window }} as (
    select
        {{ group_by_cols | join(', ') }},
        {% for metric in metric_cols %}
        sum({{ metric }}) as {{ metric }}_{{ window }}_day,
        avg({{ metric }}) as avg_{{ metric }}_{{ window }}_day
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from source_data
    where datediff('day', {{ timestamp_col }}, current_date()) <= {{ window }}
    group by {{ group_by_cols | join(', ') }}
){% if not loop.last %},{% endif %}
{% endfor %}

{% for window in windows %}
{% if loop.first %}
select
    source_data.{{ group_by_cols | join(', source_data.') }},
    {% for window in windows %}
    {% for metric in metric_cols %}
    coalesce(window_{{ window }}.{{ metric }}_{{ window }}_day, 0) as {{ metric }}_{{ window }}_day,
    coalesce(window_{{ window }}.avg_{{ metric }}_{{ window }}_day, 0) as avg_{{ metric }}_{{ window }}_day{% if not loop.last %},{% endif %}
    {% endfor %}{% if not loop.last %},{% endif %}
    {% endfor %}
from source_data
{% endif %}
left join window_{{ window }} 
    on {% for col in group_by_cols %}
    source_data.{{ col }} = window_{{ window }}.{{ col }} {% if not loop.last %}and{% endif %}
    {% endfor %}
{% if not loop.last %}{% endif %}
{% endfor %}

{% endmacro %}