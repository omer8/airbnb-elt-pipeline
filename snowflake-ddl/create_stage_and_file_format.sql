-- Set context
USE WAREHOUSE airbnb_wh;
USE DATABASE airbnb_db;

-- Create tables in RAW schema
USE SCHEMA staging;

-- Create file format for CSV files
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '', 'NA')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Create external stage for S3
CREATE STAGE airbnb_s3_stage
  STORAGE_INTEGRATION = s3_airbnb_integration  
  URL = 's3://snowflake-bucket-omar/source/';
    

-- List files to verify connection
LIST @airbnb_s3_stage;