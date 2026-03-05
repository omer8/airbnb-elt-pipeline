{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_id',
    incremental_strategy = 'merge'
    )
}}

SELECT     
    listing_id,
    property_type,
    room_type,
    city,
    country,
    accommodates,
    bedrooms,
    bathrooms,
    price_per_night_tag,
    created_at AS listings_created_at

FROM {{ ref('silver_listings') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(listings_created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}