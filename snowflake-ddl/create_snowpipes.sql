USE DATABASE airbnb_db;
USE SCHEMA staging;


CREATE OR REPLACE PIPE pipe_raw_hosts
AUTO_INGEST = TRUE
AS
COPY INTO raw_hosts
FROM (
    SELECT 
        $1::NUMBER,
        $2::STRING,
        $3::DATE,
        $4::BOOLEAN,
        $5::NUMBER,
        CURRENT_TIMESTAMP()
    FROM @airbnb_s3_stage
)
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*hosts.*\.csv';


CREATE OR REPLACE PIPE pipe_raw_listings
AUTO_INGEST = TRUE
AS
COPY INTO raw_listings
FROM (
    SELECT 
        $1::NUMBER,
        $2::NUMBER,
        $3::STRING,
        $4::STRING,
        $5::STRING,
        $6::STRING,
        $7::NUMBER,
        $8::NUMBER,
        $9::NUMBER,
        $10::NUMBER,
        CURRENT_TIMESTAMP()
    FROM @airbnb_s3_stage
)
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*listings.*\.csv';


CREATE OR REPLACE PIPE pipe_raw_bookings
AUTO_INGEST = TRUE
AS
COPY INTO raw_bookings
FROM (
    SELECT 
        $1::STRING,
        $2::NUMBER,
        $3::TIMESTAMP,
        $4::NUMBER,
        $5::NUMBER,
        $6::NUMBER,
        $7::NUMBER,
        $8::STRING,
        CURRENT_TIMESTAMP()
    FROM @airbnb_s3_stage
)
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*bookings.*\.csv';