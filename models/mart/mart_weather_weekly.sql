SELECT airport_code,
       date_year,
       cw,
       AVG(avg_temp_c) AS avg_temp_c,
       MIN(min_temp_c) AS min_temp_c,
       MAX(max_temp_c) AS max_temp_c,
       SUM(precipitation_mm) AS total_precipitation_mm,
       max(max_snow_mm) AS max_snowday_mm,
       avg(avg_wind_speed) AS wind_speed_weekly,
       MAX(avg_peakgust) as strongest_gust_week,
       AVG(avg_pressure_hpa) as average_pressure_week_hpa,
       SUM(sun_minutes) AS total_sunshine_hours_week,
       MIN(season) AS current_season
FROM {{ref('prep_weather_daily')}}
GROUP BY airport_code, date_year, cw
ORDER BY airport_code, date_year, cw