{% snapshot bookings_snapshot %}

{{
   config(
       target_database='airbnb_db',
       target_schema='snapshots',
       unique_key='booking_id',
       strategy='timestamp',
       updated_at='created_at',
       dbt_valid_to_current="to_date('9999-12-31')"
   )
}}

SELECT 
    *
FROM 
    {{ ref('silver_bookings') }}

{% endsnapshot %}