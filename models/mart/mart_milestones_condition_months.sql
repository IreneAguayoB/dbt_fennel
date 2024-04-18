WITH prep_milestone_info AS(
    SELECT * FROM {{ref('prep_milestone')}}
)

SELECT month_of_year
      ,number_of_month
      ,city
      ,round(AVG(avg_temp_c), 2):: FLOAT AS avg_temp
      ,round(AVG(max_temp_c), 2):: FLOAT AS avg_max_temp
      ,round(AVG(min_temp_c), 2):: FLOAT AS avg_min_temp
      ,round(AVG(avg_humidity), 2):: FLOAT AS avg_humidity
      ,AVG(sun_hours) AS avg_sun_hours
      ,SUM(daily_will_it_rain):: INT AS rainy_days
      ,SUM(daily_will_it_snow):: INT AS snowy_days
      ,COUNT(*) FILTER (WHERE condition_text ILIKE 'sunny') AS sunny_day
      ,MODE() within GROUP(ORDER BY condition_text) AS most_freq_condition
FROM prep_milestone_info
GROUP BY month_of_year,number_of_month,city
ORDER BY city,number_of_month