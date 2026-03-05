{{
    config(
        cluster_by=['booking_date'], 
        post_hook=[
            "GRANT SELECT ON {{ this }} TO ROLE ANALYST_ROLE"
        ]
    )
}}

WITH fact AS (
    SELECT * FROM {{ ref('fact') }}
),

bookings AS (
    SELECT * FROM {{ ref('dim_bookings') }}
),

listings AS (
    SELECT * FROM {{ ref('dim_listings') }}
),

hosts AS (
    SELECT * FROM {{ ref('dim_hosts') }}
)

SELECT 
    -- FACT ATTRIBUTES
    f.total_booking_amount,
    f.price_per_night,
    f.response_rate,

    -- BOOKING ATTRIBUTES
    f.booking_id,
    b.booking_date,
    b.booking_status,
    b.booking_created_at,

    -- LISTING ATTRIBUTES
    f.listing_id,
    l.property_type,
    l.room_type,
    l.city,
    l.country,
    l.accommodates,
    l.bedrooms,
    l.bathrooms,
    l.price_per_night_tag,
    listings_created_at,
    
    -- HOST ATTRIBUTES
    f.host_id,
    h.host_name,
    h.host_since,
    h.is_superhost,
    h.response_rate_quality,
    hosts_created_at

FROM fact f
LEFT JOIN bookings b ON f.booking_id = b.booking_id
LEFT JOIN listings l ON f.listing_id = l.listing_id
LEFT JOIN hosts h ON f.host_id = h.host_id