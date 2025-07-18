WITH hurricane_departures AS (
    SELECT 
	    flight_date,
        origin AS airport,
        COUNT(*) AS total_departures,
        SUM(diverted) AS diverted,
        SUM(cancelled) AS cancelled,
        ROUND((SUM(cancelled)::numeric / COUNT(*)) * 100, 2) AS cancelled_ratio_percent
    FROM {{ ref('prep_flights') }}
    WHERE flight_date BETWEEN '2012-10-27' AND '2012-11-01'
    AND origin IN ('JFK', 'EWR', 'LGA', 'BOS', 'DEN')
    GROUP BY origin, flight_date
),
avg_departures AS (
    SELECT 
	    flight_date,
        origin AS airport,
        COUNT(*) AS total_departures,
        SUM(diverted) AS diverted,
        SUM(cancelled) AS cancelled,
        ROUND((SUM(cancelled)::numeric / COUNT(*)) * 100, 2) AS cancelled_ratio_percent
    FROM {{ ref('prep_flights') }}
    WHERE flight_date BETWEEN '2012-10-01' AND '2012-10-07'
      AND origin IN ('JFK', 'EWR', 'LGA', 'BOS', 'DEN')
    GROUP BY origin, flight_date
)
SELECT * FROM hurricane_departures
UNION ALL
SELECT * FROM avg_departures
ORDER BY airport, flight_date