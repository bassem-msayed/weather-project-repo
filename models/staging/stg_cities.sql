with source as(
    select * 
    from {{ ref('dim_city') }}
),
renamed as(
    select
        cast(loction_id as integer) as location_id,
        cast(latitude as float)     as latitude,
        cast(longitude as float)    as longitude,
        cast(elevation as float)    as elevation,
        case 
            when (cast(latitude as float) = 55.75 and cast(longitude as float) = -4.25) then 'Glasgow'
            else when (cast(latitude as float) = 25 and cast(longitude as float) = 55.25) then 'Dubai'
            else when (cast(latitude as float) = 30 and cast(longitude as float) = 31.25) then 'Cairo'
            end                     as city_name
    from source
)
select * from renamed