-- Create a warehouse for compute resources
CREATE WAREHOUSE airbnb_wh
  WAREHOUSE_SIZE = 'XSMALL'  -- Start small, can scale up later
  AUTO_SUSPEND = 300          -- Suspend after 5 minutes of inactivity
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Use the warehouse
USE WAREHOUSE airbnb_wh;

-- Create database
CREATE DATABASE airbnb_db;

-- Use the database
USE DATABASE airbnb_db;

-- Create schema
CREATE SCHEMA staging;        -- For raw data from S3