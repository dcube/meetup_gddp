USE ROLE SYSADMIN;

------------------------------------------------------------------------------
-- Create the database for the domain &{domain}
------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS &{domain};
USE DATABASE &{domain};

------------------------------------------------------------------------------
-- Create the data product &{domain}.UTILS
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS UTILS;

-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{domain}.UTILS.LANDING
  STORAGE_INTEGRATION = &{domain}_S3
  URL = '&{s3_landing_bucket}';

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
ALTER GIT REPOSITORY &{domain}.UTILS.GIT_REPO FETCH;

-- create the generic stored procedure to load raw data from csv
CREATE OR REPLACE PROCEDURE &{domain}.UTILS.LOAD_FROM_CSV(tbl_config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@&{domain}.UTILS.GIT_REPO/&{git_ref}/snowpark/dcube/raw_tables.py')
    handler='raw_tables.load_from_csv';

------------------------------------------------------------------------------
-- Create the data products &{domain}.TPCH_SF100 & TPCH_SF100_ICEBERG
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS TPCH_SF100;
CREATE SCHEMA IF NOT EXISTS TPCH_SF100_ICEBERG;

-- Deploy tpch tables
EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/tables/tables.sql
USING (domain => '&{domain}')
DRY_RUN = &{dry_run};

-- Deploy tpch load dags
EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/dags/dag_load_parallel.sql
USING (domain => '&{domain}')
DRY_RUN = &{dry_run};

EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/dags/dag_load_sequential.sql
USING (domain => '&{domain}')
DRY_RUN = &{dry_run};

/*
-- Deploy tpch analytics dags
EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/dags/dag_nlitx_parallel.sql
USING (domain => '&{domain}', git_ref => '&{git_ref}')
DRY_RUN = &{dry_run};
*/
