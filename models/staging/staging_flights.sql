SELECT
    f.*,

    -- Origin airport info
    a_origin.name     AS origin_name,
    a_origin.city     AS origin_city,
    a_origin.country  AS origin_country,
    a_origin.lat      AS origin_lat,
    a_origin.lon      AS origin_lon,
    a_origin.alt      AS origin_alt,
    a_origin.tz       AS origin_tz,
    a_origin.dst      AS origin_dst,

    -- Destination airport info
    a_dest.name       AS dest_name,
    a_dest.city       AS dest_city,
    a_dest.country    AS dest_country,
    a_dest.lat        AS dest_lat,
    a_dest.lon        AS dest_lon,
    a_dest.alt        AS dest_alt,
    a_dest.tz         AS dest_tz,
    a_dest.dst        AS dest_dst

FROM {{ source('flights_data', 'flights_filtered') }} AS f
LEFT JOIN {{ source('flights_data', 'airports') }} AS a_origin ON f.origin = a_origin.faa
LEFT JOIN {{ source('flights_data', 'airports') }} AS a_dest   ON f.dest   = a_dest.faa