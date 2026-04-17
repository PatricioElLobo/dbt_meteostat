WITH departures AS (
    SELECT origin AS faa,
           dest AS connection,
           cancelled,
           diverted,
           'departure' AS movement_type
    FROM {{ref('prep_flights')}}
),
arrivals AS (
    SELECT dest AS faa,
           origin AS connection,
           cancelled,
           diverted,
           'arrival' AS movement_type
    FROM {{ref('prep_flights')}}
),
all_movements AS (
    SELECT * FROM departures
    UNION ALL
    SELECT * FROM arrivals
),
airport_stats AS (
    SELECT faa,
           COUNT(DISTINCT CASE WHEN movement_type = 'departure' THEN connection END) AS unique_departure_connections,
           COUNT(DISTINCT CASE WHEN movement_type = 'arrival' THEN connection END) AS unique_arrival_connections,
           COUNT(*) AS total_planned,
           SUM(cancelled) AS total_cancelled,
           SUM(diverted) AS total_diverted,
           SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS total_occurred
    FROM all_movements
    GROUP BY faa
)
SELECT s.faa,
       a.name,
       a.city,
       a.country,
       s.unique_departure_connections,
       s.unique_arrival_connections,
       s.total_planned,
       s.total_cancelled,
       s.total_diverted,
       s.total_occurred
FROM airport_stats s
LEFT JOIN {{ref('prep_airports')}} a ON s.faa = a.faa
ORDER BY total_planned DESC;