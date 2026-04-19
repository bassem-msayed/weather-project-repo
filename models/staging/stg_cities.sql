with source as(
    select * 
    from {{ source('meteo_raw', 'dim_city') }}
),
renamed as(
    select
        cast(location_id as integer)    as location_id,
        cast(latitude as float64)       as latitude,
        cast(longitude as float64)      as longitude,
        cast(elevation as float64)      as elevation,
        case 
            when (cast(latitude as float64) = 55.75 and cast(longitude as float) = -4.25)   then 'Glasgow'
            when (cast(latitude as float64) = 25 and cast(longitude as float) = 55.25)      then 'Dubai'
            when (cast(latitude as float64) = 30 and cast(longitude as float) = 31.25)      then 'Cairo'
            end                         as city_name
    from source
)
select * from renamed