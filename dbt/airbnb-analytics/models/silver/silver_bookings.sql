{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    incremental_strategy = 'merge'
    )
}}

SELECT 
    booking_id,
    listing_id,
    booking_date,
    (ROUND((nights_booked * booking_amount) + service_fee + cleaning_fee, 2)) AS total_booking_amount,
    booking_status,
    created_at
FROM 
    {{ ref('bronze_bookings') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}