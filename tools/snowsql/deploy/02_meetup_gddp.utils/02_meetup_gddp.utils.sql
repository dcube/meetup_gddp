------------------------------------------------------------------------------
-- Create the data product MEETUP_GDDP.UTILS
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE MEETUP_GDDP.UTILS;

-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS MEETUP_GDDP.UTILS.LANDING
  STORAGE_INTEGRATION = MEETUP_GDDP_S3
  URL = 's3://&{s3_bucket_landing}/';

-- create file format for CSV with | column delimiter
CREATE FILE FORMAT IF NOT EXISTS MEETUP_GDDP.UTILS.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;

-- create git repo
CREATE GIT REPOSITORY IF NOT EXISTS MEETUP_GDDP.UTILS.GIT_REPO
  API_INTEGRATION = MEETUP_GDDP_GIT
  ORIGIN = '&{git_repo_uri}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY MEETUP_GDDP.UTILS.GIT_REPO FETCH

-- create the generic stored procedure to load raw data from csv
CREATE PROCEDURE IF NOT EXISTS MEETUP_GDDP.UTILS.LOAD_FROM_CSV(config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@MEETUP_GDDP.UTILS.GIT_REPO/branches/"feature/setting_up_project"/src/dcube/snowpark/table_functions.py')
    handler='raw_data_csv_loader.load_raw_data';
