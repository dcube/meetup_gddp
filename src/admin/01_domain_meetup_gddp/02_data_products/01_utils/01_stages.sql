------------------------------------------------------------------------------
-- manage stages
------------------------------------------------------------------------------

USE ROLE SYSADMIN;

-- create external stages on s3 buckets
CREATE STAGE IF NOT EXISTS &{data_domain}.&{data_product}.LANDING
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = '&{s3_bucket_landing}';>

CREATE STAGE IF NOT EXISTS &{data_domain}.&{data_product}.LAKEHOUSE
  STORAGE_INTEGRATION = &{data_domain}_S3
  URL = '&{s3_bucket_lake}';>
