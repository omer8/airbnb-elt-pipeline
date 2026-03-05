-- Test: Ensure raw tables have data
WITH raw_counts AS (
    SELECT 'raw_listings' AS table_name, COUNT(*) AS row_count
    FROM {{ source('airbnb_raw', 'raw_listings') }}
    
    UNION ALL
    
    SELECT 'raw_bookings' AS table_name, COUNT(*) AS row_count
    FROM {{ source('airbnb_raw', 'raw_bookings') }}
    
    UNION ALL
    
    SELECT 'raw_hosts' AS table_name, COUNT(*) AS row_count
    FROM {{ source('airbnb_raw', 'raw_hosts') }}
)

SELECT 
    table_name,
    row_count
FROM raw_counts
WHERE row_count = 0