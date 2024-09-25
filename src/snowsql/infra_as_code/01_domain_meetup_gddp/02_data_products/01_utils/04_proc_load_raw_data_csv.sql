------------------------------------------------------------------------------
-- Stored proc LOAD_RAW_DATA_CSV
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- create the generic stored procedure to load raw data from csv
CREATE PROCEDURE IF NOT EXISTS &{data_domain}.UTILS.LOAD_RAW_DATA_CSV(config variant)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@&{data_domain}.UTILS.GIT_REPO/branches/"feature/setting_up_project"/src/snowpark/data_loader/raw_data_csv_loader.py')
    handler='raw_data_csv_loader.load_raw_data';
