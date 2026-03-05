{{
  config(
    materialized = 'incremental',
    unique_key = 'host_id',
    incremental_strategy = 'merge'
    )
}}

SELECT     
    host_id,
    host_name,
    host_since,
    is_superhost,
    response_rate_quality,
    created_at AS hosts_created_at

FROM {{ ref('silver_hosts') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT COALESCE(MAX(hosts_created_at), '1900-01-01') FROM {{ this }}) 
{% endif %}