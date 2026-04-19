with source as(
    select *
    from ref{{ 'fct_open_meteo_historical_data' }}
),
renamed as(
    select 
        cast(location_id as integer)                        as location_id,
        cast(time as date)                                  as date,
        cast(temperature_2m_max______C_ as float)           as temp_max,
        cast(temperature_2m_min______C_ as float)           as temp_min,
        cast(et0_fao_evapotranspiration__mm_ as float)      as evapotranspiration_mm,
        cast(shortwave_radiation_sum__MJ_m_____ as float)   as solar_radiation_mj,
        cast(precipitation_sum__mm_ as float)               as percipitation_mm
    from source
    where time is not null
)
select * from renamed