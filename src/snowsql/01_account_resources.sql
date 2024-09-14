!define data_domain=MEETUP_GDDP

-- to be replaced by secrets vars
!define s3_aws_role_arn=arn:aws:iam::203918839842:role/snowflake_dcube_partner
!define s3_bucket_lake=s3://st-fbr-lake-euw3-001/
!define s3_bucket_landing=s3://st-fbr-landing-euw3-001/

------------------------------------------------------------------------------
-- manage storage integration on S3 for the data domain
------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION IF NOT EXISTS &{data_domain}_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '&{s3_aws_role_arn}'
  STORAGE_ALLOWED_LOCATIONS = ('&{s3_bucket_lake}', '&{s3_bucket_landing}')
  ;

-- retrieve storage integration info to update aws role trust relastionshio
desc integration &{data_domain}_S3;

-- test access to buckets
select SYSTEM$VALIDATE_STORAGE_INTEGRATION('&{data_domain}_S3', '&{s3_bucket_landing}/', 'validate_all.txt', 'all');
select SYSTEM$VALIDATE_STORAGE_INTEGRATION('&{data_domain}_S3', '&{s3_bucket_lake}/', 'validate_all.txt', 'all');

GRANT USAGE ON INTEGRATION &{data_domain}_S3 TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

------------------------------------------------------------------------------
-- manage warehouses for the data domain
------------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS LOAD_XS
  WAREHOUSE_SIZE=XSMALL
  MAX_CLUSTER_COUNT=10
  AUTO_SUSPEND = 0
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS LOAD_M
  WAREHOUSE_SIZE=MEDIUM
  MAX_CLUSTER_COUNT=10
  AUTO_SUSPEND = 0
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

------------------------------------------------------------------------------
-- manage git integration for the data domain
------------------------------------------------------------------------------
--CREATE API INTEGRATION IF NOT EXISTS &{data_domain}_GIT
CREATE OR REPLACE API INTEGRATION &{data_domain}_GIT
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('&{git_repo_uri}')
  ENABLED = TRUE;
