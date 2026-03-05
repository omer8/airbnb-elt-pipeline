-- Set context
USE WAREHOUSE airbnb_wh;
USE DATABASE airbnb_db;

-- Create tables in RAW schema
USE SCHEMA staging;

-- Load Hosts Data
COPY INTO raw_hosts
FROM (
    SELECT 
        $1::NUMBER,              -- host_id
        $2::STRING,              -- host_name
        $3::DATE,                -- host_since
        $4::BOOLEAN,             -- is_superhost
        $5::NUMBER,              -- response_rate
        CURRENT_TIMESTAMP()      -- created_at
    FROM @airbnb_s3_stage/hosts/hosts.csv
)
FILE_FORMAT = csv_format
ON_ERROR = abort_statement
PURGE = FALSE;

-- Load Listings Data
COPY INTO raw_listings
FROM (
    SELECT 
        $1::NUMBER,              -- listing_id
        $2::NUMBER,              -- host_id
        $3::STRING,              -- property_type
        $4::STRING,              -- room_type
        $5::STRING,              -- city
        $6::STRING,              -- country
        $7::NUMBER,              -- accommodates
        $8::NUMBER,              -- bedrooms
        $9::NUMBER,              -- bathrooms
        $10::NUMBER,             -- price_per_night
        CURRENT_TIMESTAMP()      -- created_at
    FROM @airbnb_s3_stage/listings/listings.csv
)
FILE_FORMAT = csv_format
ON_ERROR = abort_statement
PURGE = FALSE;


-- Load Bookings Data
COPY INTO raw_bookings 
FROM (
    SELECT 
        $1::STRING,              -- booking_id
        $2::NUMBER,              -- listing_id
        $3::TIMESTAMP,           -- booking_date
        $4::NUMBER,              -- nights_booked
        $5::NUMBER,              -- booking_amount
        $6::NUMBER,              -- cleaning_fee
        $7::NUMBER,              -- service_fee
        $8::STRING,              -- booking_status
        CURRENT_TIMESTAMP()      -- created_at
    FROM @airbnb_s3_stage/bookings/bookings.csv
)
FILE_FORMAT = csv_format
ON_ERROR = abort_statement
PURGE = FALSE;

