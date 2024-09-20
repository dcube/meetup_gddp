------------------------------------------------------------------------------
-- manage file formats
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- create file format for CSV with | column delimiter
CREATE FILE FORMAT IF NOT EXISTS &{data_domain}.&{data_product}.CSV_FMT1
    TYPE = 'csv'
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'YYYY-MM-DD'
    PARSE_HEADER = true;>
