WITH prep_milestone_info AS(
    SELECT * FROM {{ref('prep_milestone')}}
)
SELECT day_of_week
      ,city
      ,SUM(daily_will_it_rain):: INT AS rainy_days
      ,SUM(daily_will_it_snow):: INT AS snowy_days
      ,COUNT(*) FILTER (WHERE condition_text ILIKE 'sunny') AS sunny_day
FROM prep_milestone_info
GROUP by day_of_week, city
order by rainy_days desc