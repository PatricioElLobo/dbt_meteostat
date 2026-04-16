WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features_hourly AS (
    SELECT *
    , timestamp::DATE AS date -- only date (year-month-day) as DATE data type
    , timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMMonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'FMDay') AS weekday        -- weekday name as TEXT            
        , DATE_PART('Day', timestamp) AS date_day
    	, DATE_PART('Month', timestamp) AS date_month
    	, DATE_PART('Year', timestamp) AS date_year
    	, DATE_PART('Week', timestamp) AS cw
    FROM hourly_data
),
add_more_features_hourly AS (
    SELECT *
    	,(CASE 
    		WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'Night'
    		WHEN time BETWEEN '06:00:00' AND '18:00:00' THEN 'Day'
    		WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'Evening'
    	END) AS day_part
    FROM add_features_hourly
)
    
SELECT *
FROM add_more_features_hourly