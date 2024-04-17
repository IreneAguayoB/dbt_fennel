WITH forecast_day_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_day')}}
),
add_features AS (
    SELECT *
        ,...(...) AS day_of_month -- day of month as a number
        ,...(...) AS month_of_year -- month name as a text
        ,...(...) AS year -- year as a number
        ,...(...) AS day_of_week -- weekday name as text
        ,...(...) AS week_of_year -- calender week number as number
        ,...(...) AS year_and_week -- year-calenderweek as text like '2024-43'

    FROM forecast_day_data
)
SELECT *
FROM add_features