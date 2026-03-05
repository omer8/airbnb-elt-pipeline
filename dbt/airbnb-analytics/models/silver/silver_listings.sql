{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_id',
    incremental_strategy = 'merge'
    )
}}  

SELECT 
    listing_id,
    host_id,
    property_type,
    room_type,
    city,
    country,
    accommodates,
    bedrooms,
    bathrooms,
    price_per_night,
    {{ tag('price_per_night') }} AS price_per_night_tag,
    created_at
FROM 
    {{ ref('bronze_listings') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}