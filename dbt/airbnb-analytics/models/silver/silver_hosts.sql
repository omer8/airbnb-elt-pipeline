{{
  config(
    materialized = 'incremental',
    unique_key = 'host_id',
    incremental_strategy = 'merge'
    )
}}

SELECT 
    host_id,
    {{ trimmer('host_name') }} AS host_name,
    host_since,
    is_superhost,
    response_rate,
    CASE 
        WHEN response_rate > 95 THEN 'Excellent'
        WHEN response_rate > 80 THEN 'Good'
        WHEN response_rate > 60 THEN 'Fair'
        ELSE 'Poor'
    END AS response_rate_quality,
    created_at
FROM 
    {{ ref('bronze_hosts') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}