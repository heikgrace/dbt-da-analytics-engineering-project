WITH hurricane_departures AS (
    SELECT 
        date_trunc('hour', flight_date) AS flight_hour,
        origin AS airport,
        COUNT(*) AS total_departures,
        SUM(diverted) AS diverted,
        SUM(cancelled) AS cancelled,
        ROUND((SUM(cancelled)::numeric / COUNT(*)) * 100, 2) AS cancelled_ratio_percent
    FROM {{ ref('prep_flights') }}
    WHERE flight_date BETWEEN '2012-10-15' AND '2012-11-15'
      AND origin IN ('JFK', 'EWR', 'LGA', 'BOS', 'DEN'
      )
    GROUP BY origin, date_trunc('hour', flight_date)
),
weather_data AS (
    SELECT 
        airport_code,
        date_trunc('hour', timestamp) AS flight_hour,
        wind_speed_kmh,
        pressure_hpa
    FROM {{ ref('prep_weather_hourly') }}
)
SELECT 
    hd.flight_hour,
    hd.airport,
    hd.total_departures,
    hd.cancelled,
    hd.cancelled_ratio_percent,
    wd.wind_speed_kmh,
    wd.pressure_hpa
FROM hurricane_departures hd
LEFT JOIN weather_data wd
    ON hd.airport = wd.airport_code
   AND hd.flight_hour = wd.flight_hour
	ORDER BY airport DESC
