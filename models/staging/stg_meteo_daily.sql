with source as(
    select *
    from {{ source('meteo_raw', 'fct_open_meteo_historical_data') }}
),
renamed as(
    select 
        cast(location_id                                           as integer)  as location_id,
        cast(time                                                  as date)     as date,
        cast(temperature_2m_max______C_                            as float64)  as temp_max,
        cast(temperature_2m_min______C_                            as float64)  as temp_min,
        cast(nullif(et0_fao_evapotranspiration__mm_,        'NaN') as float64)  as evapotranspiration_mm,
        cast(nullif(shortwave_radiation_sum__MJ_m_____,     'NaN') as float64)  as solar_radiation_mj,
        cast(nullif(precipitation_sum__mm_,                 'NaN') as float64)  as precipitation_mm
    from source
    where time is not null
)
select * from renamed