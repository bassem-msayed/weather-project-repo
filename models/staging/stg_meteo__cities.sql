-- This staging model reads directly from the dim city CSV file in seeds folder.
with source as (
    select * from {{ ref('dim_city') }}
),
renamed as (
    select
        cast(location_id as integer) as location_id,
        cast(latitude    as float64) as latitude,
        cast(longitude   as float64) as longitude,
        --cast(elevation   as float64) as elevation,
        city_name
    from source
)
select * from renamed