SELECT * FROM {{ref('staging_location')}}
JOIN {{ref('prep_forecast_day')}}
using('city')