USE ROLE ACCOUNTADMIN;

-- Set context
USE WAREHOUSE airbnb_wh;
USE DATABASE airbnb_db;

-- Create tables in RAW schema
USE SCHEMA staging;

-- create storage integration
CREATE STORAGE INTEGRATION s3_airbnb_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-role' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://omar-airbnb-data/raw/');

-- Get the IAM user ARN that Snowflake created
DESC STORAGE INTEGRATION s3_airbnb_integration;

-- Update the storage integration with actual role ARN
ALTER STORAGE INTEGRATION s3_airbnb_integration
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::085263702657:role/snowflake-s3-role';