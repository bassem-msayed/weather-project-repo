with base as (
    select * from {{ ref('int_meteo__joined') }}
),
annual as (
    select
        city_name,
        year_num,
        round(avg(temp_max),2)                                      as avg_temp_max,
        round(avg(temp_min),2)                                      as avg_temp_min,
        round(avg(solar_radiation_mj),2)                            as avg_solar_radiation_mj,
        round(avg(precipitation_mm),2)                              as avg_precipcation_mm,
        round(avg(evapotranspiration_mm),2)                         as avg_evapotranspiration_mm,
        round(avg(precipitation_mm) - avg(evapotranspiration_mm),2) as water_deficit_mm,
    from base
    group by city_name, year_num
)
select * 
from annual
