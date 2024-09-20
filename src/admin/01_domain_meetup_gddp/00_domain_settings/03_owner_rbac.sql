------------------------------------------------------------------------------
-- Manage grants on account resources for the domain owners
------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION &{data_domain}_S3 TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON INTEGRATION &{data_domain}_GIT TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON WAREHOUSE MANAGE TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON WAREHOUSE LOAD TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON WAREHOUSE ANALYSIS TO ROLE &{data_domain}_OWN;>
GRANT EXECUTE TASK ON ACCOUNT TO ROLE &{data_domain}_OWN;

------------------------------------------------------------------------------
-- grant ownership on the database objects to the domain owners
--------------------------------------------------------------------<---------
USE ROLE SECURITYADMIN;
-- ownership for all objects create into db and schemas to &{data_domain}_OWN
GRANT ALL PRIVILEGES in DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON ALL SCHEMAS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE SCHEMAS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE TABLES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE VIEWS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE PROCEDURES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE FUNCTIONS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE TASKS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE FILE FORMATS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE GIT REPOSITORIES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE NOTEBOOKS IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
GRANT OWNERSHIP ON FUTURE STAGES IN DATABASE &{data_domain} TO ROLE &{data_domain}_OWN;>
USE ROLE SYSADMIN;
