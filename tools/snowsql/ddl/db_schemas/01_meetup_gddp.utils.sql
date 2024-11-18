------------------------------------------------------------------------------
-- Create the data product &{data_domain}.UTILS
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
set data_domain='MEETUP_GDDP'

CREAtE DATABASE IF NOT EXISTS &{data_domain};

CREATE SCHEMA IF NOT EXISTS &{data_domain}.UTILS;

-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{data_domain}.UTILS.LANDING
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = 's3://&{s3_bucket_landing}/';

-- create file format for CSV with | column delimiter
CREATE FILE FORMAT IF NOT EXISTS &{data_domain}.UTILS.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;

-- create git repo
CREATE GIT REPOSITORY IF NOT EXISTS &{data_domain}.UTILS.GIT_REPO
  API_INTEGRATION = &{data_domain}_GIT
  ORIGIN = '&{git_repo_uri}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY &{data_domain}.UTILS.GIT_REPO FETCH

-- create the generic stored procedure to load raw data from csv
CREATE PROCEDURE IF NOT EXISTS &{data_domain}.UTILS.LOAD_FROM_CSV(config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@&{data_domain}.UTILS.GIT_REPO/branches/"feature/setting_up_project"/src/dcube/snowpark/table_functions.py')
    handler='raw_data_csv_loader.load_raw_data';
