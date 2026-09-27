{{
    config(
        materialized='table'
    )
}}

with
    source as (
        select * from {{ source("meteo_raw", "raw_weather_daily") }}

        {% if is_incremental() %}
            where date > (select date_sub(max(date), interval 14 days) from {{ this }})
        {% endif %}
    ),

    renamed as (
        select
            cast(location_id - 1 as integer) as location_id, -- Adjusting location_id 1, 2, 3 -> 0, 1, 2 to align with seed_cities dimension.
            cast(date as date) as date,
            cast(temp_max as float64) as temp_max,
            cast(temp_min as float64) as temp_min,
            safe_cast(evapotranspiration_mm as float64) as evapotranspiration_mm,
            safe_cast(solar_radiation_mj as float64) as solar_radiation_mj,
            safe_cast(precipitation_mm as float64) as precipitation_mm,
            cast(extracted_at as timestamp) as extracted_at
        from source
        where date is not null
    ),

    deduplicated as (
        select
            location_id,
            date,
            temp_max,
            temp_min,
            evapotranspiration_mm,
            solar_radiation_mj,
            precipitation_mm,
            extracted_at
        from renamed
        qualify row_number() over(
            partition by location_id, date
            order by extracted_at desc
        ) = 1
    )

select *
from deduplicated