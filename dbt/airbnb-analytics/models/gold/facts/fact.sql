{{
    config(
        materialized='incremental',
        unique_key='booking_id',
        incremental_strategy='merge'
    )
}}

SELECT 
    b.booking_id,
    b.total_booking_amount,
    b.created_at,
    l.listing_id,
    l.price_per_night,
    h.host_id,
    h.response_rate
FROM {{ ref('silver_bookings') }} AS b
LEFT JOIN {{ ref('silver_listings') }} AS l 
    ON b.listing_id = l.listing_id
LEFT JOIN {{ ref('silver_hosts') }} AS h 
    ON l.host_id = h.host_id

{% if is_incremental() %}
    WHERE b.created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}