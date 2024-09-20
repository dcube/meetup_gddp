!define data_product=TPCH_SF100

------------------------------------------------------------------------------
-- Create the database for the domain
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS &{data_domain}.&{data_product};
