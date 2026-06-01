{{config(severity='warn')}}

select
    city_name,
    year_num,
    avg_temp_min,
    avg_temp_max

from {{ ref('mart_meteo__trends') }}

where avg_temp_min > avg_temp_max