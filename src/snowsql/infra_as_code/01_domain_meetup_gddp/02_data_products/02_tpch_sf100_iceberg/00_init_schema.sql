------------------------------------------------------------------------------
-- Create the database for the domain
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = '&{data_domain}_S3_LAKEHOUSE';
