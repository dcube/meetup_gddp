USE ROLE ACCOUNTADMIN;

-- ACCOUNT PARAMETERS
ALTER ACCOUNT SET
    TIMEZONE = 'Europe/Paris',
    USE_CACHED_RESULT = FALSE -- disable cached query result set for testing purpose
    ;

-- STORAGE INTEGRATION &{domain}_S3
CREATE STORAGE INTEGRATION IF NOT EXISTS &{domain}_S3
TYPE = 'EXTERNAL_STAGE',
STORAGE_PROVIDER = 'S3',
ENABLED = TRUE,
STORAGE_AWS_ROLE_ARN = '&{s3_landing_role_arn}',
STORAGE_ALLOWED_LOCATIONS = ('&{s3_landing_bucket}');
GRANT USAGE ON INTEGRATION &{domain}_S3 TO ROLE SYSADMIN;

-- API INTEGRATION &{domain}_GIT
CREATE API INTEGRATION IF NOT EXISTS &{domain}_GIT
API_PROVIDER = 'git_https_api',
API_ALLOWED_PREFIXES = ('&{git_repo_uri}'),
ENABLED = TRUE;
GRANT USAGE ON INTEGRATION &{domain}_GIT TO ROLE SYSADMIN;

-- EXTERNAL VOLUME &{domain}_S3_LAKEHOUSE
CREATE EXTERNAL VOLUME IF NOT EXISTS &{domain}_S3_LAKEHOUSE
STORAGE_LOCATIONS = (
    (
        NAME = 'S3_BUCKET_LAKE',
        STORAGE_PROVIDER = 'S3',
        STORAGE_BASE_URL = '&{s3_lake_bucket}',
        STORAGE_AWS_ROLE_ARN = '&{s3_lake_role_arn}',
        STORAGE_AWS_EXTERNAL_ID = '&{s3_lake_external_id}'
    )
);

GRANT USAGE ON VOLUME &{domain}_S3_LAKEHOUSE TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE SYSADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

-- WAREHOUSE MANAGE
CREATE OR ALTER WAREHOUSE MANAGE WAREHOUSE_SIZE = 'XSMALL',
MAX_CLUSTER_COUNT = 1,
AUTO_SUSPEND = 60,
AUTO_RESUME = TRUE,
INITIALLY_SUSPENDED = TRUE;

-- WAREHOUSE LOAD
CREATE OR ALTER WAREHOUSE LOAD WAREHOUSE_SIZE = 'MEDIUM',
MAX_CLUSTER_COUNT = 1,
AUTO_SUSPEND = 60,
AUTO_RESUME = TRUE,
INITIALLY_SUSPENDED = TRUE;

-- WAREHOUSE ANALYSIS
CREATE OR ALTER WAREHOUSE ANALYSIS WAREHOUSE_SIZE = 'MEDIUM',
MAX_CLUSTER_COUNT = 1,
AUTO_SUSPEND = 60,
AUTO_RESUME = TRUE,
INITIALLY_SUSPENDED = TRUE;
