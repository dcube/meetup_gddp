USE ROLE SYSADMIN;

USE DATABASE &{domain};

------------------------------------------------------------------------------
-- Create the data product &{domain}.CYBERSYN
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS CYBERSYN;
  -- Technical role for owners on schema &{domain}.CYBERSYN
  USE ROLE SECURITYADMIN;

  -- create technical roles
  -- create owner role
  CREATE ROLE IF NOT EXISTS TR_&{domain}_CYBERSYN_OWNR;
    -- defining privileges
    GRANT USAGE ON DATABASE &{domain} TO ROLE TR_&{domain}_CYBERSYN_OWNR;
    GRANT USAGE ON SCHEMA &{domain}.CYBERSYN TO ROLE TR_&{domain}_CYBERSYN_OWNR;
    GRANT CREATE STREAMLIT ON SCHEMA &{domain}.CYBERSYN TO ROLE TR_&{domain}_CYBERSYN_OWNR;
    GRANT CREATE STAGE ON SCHEMA &{domain}.CYBERSYN TO ROLE TR_&{domain}_CYBERSYN_OWNR;

    -- role inherentence to functional roles
    GRANT ROLE TR_&{domain}_CYBERSYN_OWNR TO ROLE FR_&{domain}_ANALYSTS;

  USE ROLE SYSADMIN;
