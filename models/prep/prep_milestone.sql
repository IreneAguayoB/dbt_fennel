WITH one_year_clima_info AS(
    SELECT date,
           city,
           country,
           max_temp_c,
           min_temp_c,
           avg_temp_c,
           total_precip_mm,
           total_snow_cm,
           avg_humidity,
           daily_will_it_rain,
           daily_will_it_snow,
           condition_text,
           uv,
           TO_TIMESTAMP(sunrise, 'HH12:MI AM'):: time as sunrise,
           TO_TIMESTAMP(sunset, 'HH12:MI AM'):: time as sunset
    FROM {{ref("staging_forecast_day")}}
    -- removing null values
    WHERE date IS NOT null
),
add_features AS(
    SELECT * 
    -- date info
    ,DATE_PART('day', date):: INT AS day_of_month 
    ,TO_CHAR(date, 'month') AS month_of_year
    ,DATE_PART('month', date):: INT AS number_of_month
    ,DATE_PART('year', date):: INT as year
    ,TO_CHAR(date, 'day') as day_of_week
    ,DATE_PART('week', date):: INT as week_of_year
    -- sun hours
    ,sunset - sunrise as sun_hours
    
    FROM one_year_clima_info

) SELECT * FROM add_features