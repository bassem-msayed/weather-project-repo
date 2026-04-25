with base as (
    select * from {{ ref('int_meteo__joined') }}
),
annual as (
    select
        city_name,
        year_num,
        floor(year_num / 10) *10                                    as decade,
        round(avg(temp_max),2)                                      as avg_temp_max,
        round(avg(temp_min),2)                                      as avg_temp_min,
        round(avg( (temp_max + temp_min)/2 )                        as avg_temp_mean,
        round(avg(solar_radiation_mj),2)                            as avg_solar_radiation_mj,
        round(avg(precipitation_mm),2)                              as avg_precipcation_mm,
        round(avg(evapotranspiration_mm),2)                         as avg_evapotranspiration_mm,
        round(avg(precipitation_mm) - avg(evapotranspiration_mm),2) as water_deficit_mm,
    from base
    group by city_name, year_num
),
with_windows as(
    select
        *,
        -- Rolling 10 years average temprature
        avg(avg_temp_mean) over(
            partition by city_name
            order by year_num
            rows between 9 preceeding and current row)          as rolling_10yr_avg_temp,
        
        -- Previous year temperature & YoY delta
        lag(avg_temp_mean) over(
            partition by city_name
            order by year_num)                                  as prev_year_avg_temp,
        
        avg_temp_mean - lag(avg_temp_mean) over (
                            partition by city_name 
                            order by year_num)                  as yoy_temp_delta,
        
        -- Hotest year rank per city
        rank() over(
            partition by city_name
            order by avg_temp_mean desc)                        as hottest_year_rank,
        
        -- Decade aveage temp by city
        avg(avg_temp_mean) over(
            partition by city_name, decade)                     as decade_avg_temp,

        -- baseline: 1940, decade average by city
        first_value(decade_avg_temp) over(
            partition by city_name
            order by decade)                                    as baseline_decade_avg,
        
        -- 1.5 degree Paris Agreement flag
        case 
            when decade_avg_temp - first_value(decade_avg_temp) over(
                                                partition by city_name
                                                order by decade)
                >= 1.5 then true
            else false
        end                                                     as exceeds_1_5c_threshold

    from annual
)

select * 
from with_windows
