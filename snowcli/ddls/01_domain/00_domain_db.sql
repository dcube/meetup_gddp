USE ROLE SYSADMIN;

------------------------------------------------------------------------------
-- Create the database for the domain &{domain}
------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS &{domain};
USE DATABASE &{domain};

-- create domain business roles
USE ROLE SECURITYADMIN;

-- create role ops for the domain
CREATE ROLE IF NOT EXISTS FR_&{domain}_OPS;
  GRANT ROLE FR_&{domain}_OPS TO ROLE SYSADMIN;
  GRANT USAGE ON WAREHOUSE LOAD TO ROLE FR_&{domain}_OPS;

-- create role analysts for the domain
CREATE ROLE IF NOT EXISTS FR_&{domain}_ANALYSTS;
  GRANT ROLE FR_&{domain}_ANALYSTS TO ROLE SYSADMIN;
  GRANT USAGE ON WAREHOUSE ANALYSIS TO ROLE FR_&{domain}_OPS;

USE ROLE SYSADMIN;
