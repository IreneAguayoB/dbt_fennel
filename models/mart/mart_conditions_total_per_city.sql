WITH prep_milestone_info AS(
    SELECT * FROM {{ref('prep_milestone')}}
)
SELECT city
      ,SUM(daily_will_it_rain):: INT AS rainy_days
      ,COUNT(*) FILTER (WHERE condition_text ILIKE 'sunny') AS sunny_day
      ,COUNT(*) FILTER (WHERE min_temp_c > 20) AS tropical_nights
FROM prep_milestone_info
group by city