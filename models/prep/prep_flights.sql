WITH flights_reorder AS (
    SELECT *
    FROM {{ref('staging_flights')}}
)
SELECT * FROM flights_reorder