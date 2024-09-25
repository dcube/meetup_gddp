------------------------------------------------------------------------------
-- manage external volume on S3 for the data domain
------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE EXTERNAL VOLUME &{data_domain}_S3_LAKEHOUSE
   STORAGE_LOCATIONS =
      (
         (
            NAME = '&{s3_bucket_lake}'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://&{s3_bucket_lake}/'
            STORAGE_AWS_ROLE_ARN = '&{s3_aws_lake_role_arn}'
            STORAGE_AWS_EXTERNAL_ID = '&{s3_aws_lake_external_id}'
         )
      );

DESC EXTERNAL VOLUME &{data_domain}_S3_LAKEHOUSE;

USE ROLE SYSADMIN;
