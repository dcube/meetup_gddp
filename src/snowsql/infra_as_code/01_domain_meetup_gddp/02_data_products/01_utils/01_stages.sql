------------------------------------------------------------------------------
-- manage stages
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{data_domain}.UTILS.LANDING
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = 's3://&{s3_bucket_landing}/';
