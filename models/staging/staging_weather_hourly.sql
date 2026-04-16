WITH hourly_raw AS (
					SELECT airport_code
							,station_id
							,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
					FROM {{source('weather_data', 'weather_hourly_raw')}}		
),
hourly_flattened AS (
					SELECT airport_code
							,station_id
							,(json_data ->> 'time')::timestamp AS timestamp
							,(json_data ->> 'temp')::NUMERIC AS avg_temp_c
							,(json_data ->> 'dwpt')::NUMERIC AS dew_point_c
							,(json_data ->> 'rhum')::NUMERIC AS relative_humidity_percent
							,(json_data ->> 'prcp')::NUMERIC AS precipitation_mm							
							,(json_data ->> 'snow')::NUMERIC AS snowfall_cm
							,(json_data ->> 'wdir')::NUMERIC AS wind_direction_degrees
							,(json_data ->> 'wspd')::NUMERIC AS avg_wind_speed_kmh
							,(json_data ->> 'wpgt')::NUMERIC AS peak_wind_gust_kmh
							,(json_data ->> 'pres')::NUMERIC AS air_pressure_hpa
							,(json_data ->> 'tsun')::NUMERIC AS sunshine_duration_min
							,(json_data ->> 'coco')::NUMERIC AS weather_condition_code
						FROM hourly_raw
)
SELECT * FROM hourly_flattened