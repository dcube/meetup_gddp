USE ROLE &{data_domain}_OWN;
USE WAREHOUSE LOAD;
USE SCHEMA &{data_domain}.&{data_product}

-- execute the dag to load raw data
EXECUTE TASK LOAD_RAW_DATA_ENTRYPOINT;

