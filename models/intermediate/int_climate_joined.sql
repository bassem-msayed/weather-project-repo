with cities as (
    select * from {{ ref('stg_meteo__cities') }}
),
daily as (
    select * from {{ ref('stg_meteo__daily') }}
),
joined as (
    select
        c.city_name,
        d.date,
        extract(year from d.date) as year_num,
        extract(month from d.date) as month_num,
        percipitation_mm,
        solar_radiation_mj,
        evapotranspiration_mm,
        temp_min,
        temp_max
    from daily d
    left join cities c
        on d.locationid = c.locationid
)

select * from joined