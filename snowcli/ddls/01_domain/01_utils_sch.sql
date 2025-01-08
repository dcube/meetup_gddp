USE ROLE SYSADMIN;

USE DATABASE &{domain};

------------------------------------------------------------------------------
-- Create the data product &{domain}.UTILS
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS UTILS;
  -- Technical role for owners on schema &{domain}.UTILS
  USE ROLE SECURITYADMIN;

  -- create technical roles
  CREATE ROLE IF NOT EXISTS TR_&{domain}_UTILS_OWNR;
    -- defining privileges
    GRANT USAGE ON DATABASE &{domain} TO ROLE TR_&{domain}_UTILS_OWNR;
    GRANT USAGE ON SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
    GRANT USAGE ON INTEGRATION &{domain}_S3 TO ROLE FR_&{domain}_OPS;
    GRANT USAGE ON INTEGRATION &{domain}_GIT TO ROLE FR_&{domain}_OPS;
    GRANT USAGE ON VOLUME &{domain}_S3_LAKEHOUSE TO ROLE FR_&{domain}_OPS;
    EXECUTE IMMEDIATE
    $$
    BEGIN
      GRANT OWNERSHIP ON FUTURE STAGES IN SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
      GRANT OWNERSHIP ON FUTURE FILE FORMATS IN SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
      GRANT OWNERSHIP ON FUTURE GIT REPOSITORIES IN SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
      GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
      GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA &{domain}.UTILS TO ROLE TR_&{domain}_UTILS_OWNR;
    EXCEPTION
      WHEN STATEMENT_ERROR THEN
            RETURN OBJECT_CONSTRUCT('EXCEPTION', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate,
                            'TYPE', 'warning');
        WHEN OTHER THEN
            RAISE;
    END;
    $$;

    -- role inherentence to functional roles
    GRANT ROLE TR_&{domain}_UTILS_OWNR TO ROLE FR_&{domain}_OPS;

  USE ROLE SYSADMIN;

  -- create stages
  -- create external stages on s3 buckets
  CREATE STAGE IF NOT EXISTS &{domain}.UTILS.LANDING
    STORAGE_INTEGRATION = &{domain}_S3
    URL = '&{s3_landing_bucket}';

  -- create file formats
  -- create file format for CSV with | column delimiter
  CREATE FILE FORMAT IF NOT EXISTS &{domain}.UTILS.CSV_FMT1
      TYPE = 'csv'
      FIELD_DELIMITER = '|'
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      DATE_FORMAT = 'YYYY-MM-DD'
      PARSE_HEADER = true;

  -- create git repos
  -- create git repo
  CREATE GIT REPOSITORY IF NOT EXISTS &{domain}.UTILS.GIT_REPO
    API_INTEGRATION = &{domain}_GIT
    ORIGIN = '&{git_repo_uri}';

  -- fetch the git repo to update it
  ALTER GIT REPOSITORY &{domain}.UTILS.GIT_REPO FETCH;

  -- create procedures
  -- create the generic stored procedure to load raw data from csv
  CREATE OR REPLACE PROCEDURE &{domain}.UTILS.LOAD_FROM_CSV(tbl_config variant)
      returns table()
      language python
      runtime_version='3.11'
      packages=('snowflake-snowpark-python')
      imports=('@&{domain}.UTILS.GIT_REPO/&{git_ref}/snowpark/dcube/raw_tables.py')
      handler='raw_tables.load_from_csv';
