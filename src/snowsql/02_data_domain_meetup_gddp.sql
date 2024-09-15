------------------------------------------------------------------------------
-- manage database for the data domain
------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS &{data_domain};

------------------------------------------------------------------------------
-- shared objects for the domain
------------------------------------------------------------------------------
-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{data_domain}.PUBLIC.LANDING
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = '&{s3_bucket_landing}';

CREATE STAGE IF NOT EXISTS &{data_domain}.PUBLIC.LAKEHOUSE
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = '&{s3_bucket_lake}';

-- create file formats
CREATE FILE FORMAT IF NOT EXISTS &{data_domain}.PUBLIC.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;

-- git repositories
CREATE GIT REPOSITORY IF NOT EXISTS &{data_domain}.PUBLIC.GIT_REPO
  API_INTEGRATION = &{data_domain}_GIT
  ORIGIN = '&{git_repo_uri}';

-- fetch the git repo to update it
ALTER GIT REPOSITORY &{data_domain}.PUBLIC.GIT_REPO FETCH;

-- stored proc
--CREATE OR REPLACE PROC &{data_domain}.PUBLIC.LOAD_RAW_DATA_CSV

------------------------------------------------------------------------------
-- data_product TPCH_SF100
------------------------------------------------------------------------------
!define data_product=TPCH_SF100
CREATE SCHEMA IF NOT EXISTS &{data_domain}.&{data_product};
