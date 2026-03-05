{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    incremental_strategy = 'merge'
    )
}}

SELECT     
    booking_id,
    booking_date,
    booking_status,
    created_at AS booking_created_at

FROM {{ ref('silver_bookings') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(booking_created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}