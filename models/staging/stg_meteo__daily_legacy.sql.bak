with
    source as (
        select * from {{ source("meteo_raw", "fct_open_meteo_historical_data") }}
    ),
    renamed as (
        select
            cast(location_id as integer) as location_id,
            cast(time as date) as date,
            cast(temperature_max as float64) as temp_max,
            cast(temperature_min as float64) as temp_min,
            safe_cast(evapotranspiration_mm as float64) as evapotranspiration_mm,
            safe_cast(shortwave_radiation_sum as float64) as solar_radiation_mj,
            safe_cast(precipitation_sum_mm as float64) as precipitation_mm
        from source
        where time is not null
    )
select *
from renamed
