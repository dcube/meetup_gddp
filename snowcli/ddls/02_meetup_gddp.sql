USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS &{DOMAIN};
USE DATABASE &{DOMAIN};
CREATE SCHEMA UTILS;
------------------------------------------------------------------------------
-- Create the data product &{DOMAIN}.UTILS
------------------------------------------------------------------------------
-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{DOMAIN}.UTILS.LANDING
  STORAGE_INTEGRATION = &{DOMAIN}_S3
  URL = '&{S3_LANDING_BUCKET}/';

-- create file format for CSV with | column delimiter
CREATE FILE FORMAT IF NOT EXISTS &{DOMAIN}.UTILS.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;

-- create git repo
CREATE GIT REPOSITORY IF NOT EXISTS &{DOMAIN}.UTILS.GIT_REPO
  API_INTEGRATION = &{DOMAIN}_GIT
  ORIGIN = '&{GIT_REPO_URI}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY &{DOMAIN}.UTILS.GIT_REPO FETCH

-- create the generic stored procedure to load raw data from csv
CREATE PROCEDURE IF NOT EXISTS &{DOMAIN}.UTILS.LOAD_FROM_CSV(config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@&{DOMAIN}.UTILS.GIT_REPO/&{GIT_REF}/snowpark/dcube/table_functions.py')
    handler='raw_data_csv_loader.load_raw_data';

-- create the schemas tpch_sf100 and tpch_sf100_iceberg
CREATE SCHEMA IF NOT EXISTS &{DOMAIN}.TPCH_SF100;
CREATE SCHEMA IF NOT EXISTS &{DOMAIN}.TPCH_SF100_ICEBERG;

-- Deploy tpch tables
-- EXECUTE IMMEDIATE FROM @&{DOMAIN}.UTILS.GIT_REPO/&{GIT_REF}

-- Deploy tpch dags
-- EXECUTE IMMEDIATE FROM
