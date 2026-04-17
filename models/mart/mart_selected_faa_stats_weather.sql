WITH departures AS (
    SELECT origin AS faa,
           flight_date,
           cancelled,
           diverted,
           'departure' AS movement_type
    FROM {{ref('prep_flights')}}
    WHERE origin IN ('JFK', 'MIA', 'LAX')
),
arrivals AS (
    SELECT dest AS faa,
           flight_date,
           cancelled,
           diverted,
           'arrival' AS movement_type
    FROM {{ref('prep_flights')}}
    WHERE dest IN ('JFK', 'MIA', 'LAX')
),
all_movements AS (
    SELECT * FROM departures
    UNION ALL
    SELECT * FROM arrivals
),
airport_daily_stats AS (
    SELECT faa,
           flight_date,
           COUNT(DISTINCT CASE WHEN movement_type = 'departure' THEN faa END) AS unique_departure_connections,
           COUNT(DISTINCT CASE WHEN movement_type = 'arrival' THEN faa END) AS unique_arrival_connections,
           COUNT(*) AS total_planned,
           SUM(cancelled) AS total_cancelled,
           SUM(diverted) AS total_diverted,
           SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS total_occurred
    FROM all_movements
    GROUP BY faa, flight_date
)
SELECT s.faa,
       s.flight_date,
       a.name,
       a.city,
       a.country,
       s.unique_departure_connections,
       s.unique_arrival_connections,
       s.total_planned,
       s.total_cancelled,
       s.total_diverted,
       s.total_occurred,
       w.min_temp_c,
       w.max_temp_c,
       w.precipitation_mm,
       w.max_snow_mm,
       w.avg_wind_direction,
       w.avg_wind_speed,
       w.avg_peakgust
FROM airport_daily_stats s
LEFT JOIN {{ref('prep_airports')}} a ON s.faa = a.faa
LEFT JOIN {{ref('prep_weather_daily')}} w ON s.faa = w.airport_code 
                                         AND s.flight_date = w.date
ORDER BY s.flight_date, s.faa