------------------------------------------------------------------------------
-- manage storage integration on S3 for the data domain
------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION IF NOT EXISTS &{data_domain}_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '&{s3_aws_landing_role_arn}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://&{s3_bucket_landing}/')
  ;

-- retrieve storage integration info to update aws role trust relastionshio
DESC INTEGRATION &{data_domain}_S3;

-- test access to buckets
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('&{data_domain}_S3', 's3://&{s3_bucket_landing}/', 'validate_all.txt', 'all');

------------------------------------------------------------------------------
-- manage git integration for the data domain
------------------------------------------------------------------------------
CREATE API INTEGRATION IF NOT EXISTS &{data_domain}_GIT
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('&{git_repo_uri}')
  ENABLED = TRUE;

USE ROLE SYSADMIN;
