------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100_ICEBERG parallel
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100_ICEBERG in parallel
------------------------------------------------------------------------------
-- create the sequential dag to load raw data sequentially
CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  WAREHOUSE=LOAD
  AS
  SELECT 'dummy'
  ;

-- task to load region
CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_REGION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.region
    from @&{data_domain}.utils.landing/tpch-sf100/csv/region/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_NATION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.nation
    from @&{data_domain}.utils.landing/tpch-sf100/csv/nation/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_SUPPLIER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.supplier
    from @&{data_domain}.utils.landing/tpch-sf100/csv/supplier/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_PART
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.part
    from @&{data_domain}.utils.landing/tpch-sf100/csv/part/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_CUSTOMER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.customer
    from @&{data_domain}.utils.landing/tpch-sf100/csv/customer/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_PARTSUPP
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.partsupp
    from @&{data_domain}.utils.landing/tpch-sf100/csv/partsupp/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ORDERS
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.orders
    from @&{data_domain}.utils.landing/tpch-sf100/csv/orders/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_LINEITEM
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ENTRYPOINT
  AS
  copy into &{data_domain}.TPCH_SF100_ICEBERG.lineitem
    from @&{data_domain}.utils.landing/tpch-sf100/csv/lineitem/
    file_format = (
        format_name = '&{data_domain}.utils.csv_fmt1'
    )
    match_by_column_name = case_insensitive
    force = True
  ;

-- resume the DAG tasks to enable runs
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_LINEITEM RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_ORDERS RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_PARTSUPP RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_CUSTOMER RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_PART RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_SUPPLIER RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_NATION RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100_ICEBERG.LOAD_RAW_DATA_REGION RESUME;
