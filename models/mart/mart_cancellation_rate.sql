-- airport information, cancelled, date, diverted, wetterbedindung (windspeed) correlation
WITH cancellation_rate AS (
    SELECT 	
        origin,
        dest,
        flight_date,
        SUM(cancelled) AS dep_cancelled,
        SUM(diverted) AS dep_diverted
    FROM {{ ref('prep_flights') }}
    WHERE cancelled IS NOT NULL AND diverted IS NOT NULL 
	AND flight_date BETWEEN '2012-10-25' AND '2021-11-05'
    GROUP BY flight_date, origin, dest
    ORDER BY flight_date 
) 
SELECT * 
FROM cancellation_rate