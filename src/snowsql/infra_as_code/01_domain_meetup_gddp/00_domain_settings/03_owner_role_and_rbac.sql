------------------------------------------------------------------------------
-- Create the database for the domain
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS &{data_domain};

------------------------------------------------------------------------------
-- Create role for the domain owners
------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS &{data_domain}_OWN;
GRANT ROLE &{data_domain}_OWN TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

------------------------------------------------------------------------------
-- Manage grants on account resources for the domain owners
------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION &{data_domain}_S3 TO ROLE &{data_domain}_OWN;
GRANT USAGE ON INTEGRATION &{data_domain}_GIT TO ROLE &{data_domain}_OWN;
GRANT USAGE ON EXTERNAL VOLUME &{data_domain}_S3_LAKEHOUSE TO ROLE &{data_domain}_OWN;
GRANT USAGE ON WAREHOUSE MANAGE TO ROLE &{data_domain}_OWN;
GRANT USAGE ON WAREHOUSE LOAD TO ROLE &{data_domain}_OWN;
GRANT USAGE ON WAREHOUSE ANALYSIS TO ROLE &{data_domain}_OWN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE &{data_domain}_OWN;

------------------------------------------------------------------------------
-- grant ownership on the database objects to the domain owner
--------------------------------------------------------------------<---------
USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} SCHEMAS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} TABLES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} ICEBERG TABLES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} VIEWS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} PROCEDURES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} FUNCTIONS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} TASKS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} FILE FORMATS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} GIT REPOSITORIES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
GRANT OWNERSHIP ON &{owners_grants_future_all} STAGES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;
USE ROLE SYSADMIN;
