!define data_domain=MEETUP_GDDP

-- to be replaced by secrets vars
!define s3_aws_role_arn=arn:aws:iam::203918839842:role/snowflake_dcube_partner
!define s3_bucket_lake=s3://st-fbr-lake-euw3-001/
!define s3_bucket_landing=s3://st-fbr-landing-euw3-001/

------------------------------------------------------------------------------
-- Create the database for the domain
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS &{data_domain};
