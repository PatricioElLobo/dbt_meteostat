WITH route_stats AS (
    SELECT origin,
           dest,
           COUNT(*) AS total_flights,
           COUNT(DISTINCT tail_number) AS unique_airplanes,
           COUNT(DISTINCT airline) AS unique_airlines,
           ROUND(AVG(actual_elapsed_time), 2) AS avg_elapsed_time_min,
           ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
           MAX(arr_delay) AS max_arr_delay,
           MIN(arr_delay) AS min_arr_delay,
           SUM(cancelled) AS total_cancelled,
           SUM(diverted) AS total_diverted
    FROM {{ref('prep_flights')}}
    GROUP BY origin, dest
)
SELECT r.*,
       o.name AS origin_name,
       o.city AS origin_city,
       o.country AS origin_country,
       d.name AS dest_name,
       d.city AS dest_city,
       d.country AS dest_country
FROM route_stats r
LEFT JOIN {{ref('prep_airports')}} o ON r.origin = o.faa
LEFT JOIN {{ref('prep_airports')}} d ON r.dest = d.faa
ORDER BY total_flights DESC