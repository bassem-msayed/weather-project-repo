with base as (
    select * from {{ ref('int_meteo__joined') }}
),
annual as (
    select
        city_name,
        year_num,
        avg(temp_max) as avg_temp_max,
        avg(temp_min) as avg_temp_min,
        avg(solar_radiation_mj) as avg_solar_radiation_mj,
        avg(precipitation_mm) as avg_precipcation_mm,
        avg(evapotranspiration_mm) as avg_evapotranspiration_mm,
        avg(precipitation_mm) - avg(evapotranspiration_mm) as water_deficit_mm,
    from base
    group by city_name, year_num
)
select * 
from annual
