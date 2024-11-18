USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS &{domain};
USE DATABASE &{domain};

------------------------------------------------------------------------------
-- Create the data product &{domain}.UTILS
------------------------------------------------------------------------------
-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{domain}.UTILS.LANDING
  STORAGE_INTEGRATION = &{domain}_S3
  URL = 's3://&{s3_bucket_landing}/';

-- create file format for CSV with | column delimiter
CREATE FILE FORMAT IF NOT EXISTS &{domain}.UTILS.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;

-- create git repo
CREATE GIT REPOSITORY IF NOT EXISTS &{domain}.UTILS.GIT_REPO
  API_INTEGRATION = &{domain}_GIT
  ORIGIN = '&{git_repo_uri}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY &{domain}.UTILS.GIT_REPO FETCH

-- create the generic stored procedure to load raw data from csv
CREATE PROCEDURE IF NOT EXISTS &{domain}.UTILS.LOAD_FROM_CSV(config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@&{domain}.UTILS.GIT_REPO/&{GIT_REF}/src/dcube/snowpark/table_functions.py')
    handler='raw_data_csv_loader.load_raw_data';

-- create the schemas tpch_sf100 and tpch_sf100_iceberg
CREATE SCHEMA IF NOT EXISTS &{domain}.TPCH_SF100;
CREATE SCHEMA IF NOT EXISTS &{domain}.TPCH_SF100_ICEBERG;

-- Deploy tpch tables

-- Deploy tpch dags
EXECUTE IMMEDIATE FROM
