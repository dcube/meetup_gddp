USE ROLE &{data_domain}_OWN;
USE WAREHOUSE LOAD;
USE SCHEMA &{data_domain}.&{data_product}

-- execute the dag to load raw data
EXECUTE TASK RUN_TPCH_QUERIES_ENTRYPOINT;
