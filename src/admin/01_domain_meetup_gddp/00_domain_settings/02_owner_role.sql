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
