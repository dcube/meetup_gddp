------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into &{data_product} sequentially
------------------------------------------------------------------------------
USE ROLE SYSADMIN;
!define dag_task_basename=LOAD_RAW_DATA_SEQUENTIALLY

-- create the sequential dag to load raw data sequentially
CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_ENTRYPOINT
  WAREHOUSE=LOAD
  AS
  SELECT 'dummy'
  ;

-- task to load region
CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_REGION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.region",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/region/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_NATION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_REGION
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.nation",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/nation/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_SUPPLIER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_NATION
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.supplier",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/supplier/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_PART
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_SUPPLIER
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.part",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/part/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_CUSTOMER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_PART
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.customer",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/customer/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_PARTSUPP
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_CUSTOMER
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.partsupp",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/partsupp/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_ORDERS
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_PARTSUPP
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.orders",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/orders/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_LINEITEM
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.&{dag_task_basename}_ORDERS
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.lineitem",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/lineitem/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

-- resume the DAG tasks to enable runs
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_LINEITEM RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_ORDERS RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_PARTSUPP RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_CUSTOMER RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_PART RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_SUPPLIER RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_NATION RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.&{dag_task_basename}_REGION RESUME;

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into &{data_product} in parallel
------------------------------------------------------------------------------
-- create the sequential dag to load raw data sequentially
CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  WAREHOUSE=LOAD
  AS
  SELECT 'Start run &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START' as comment, current_timestamp() as event_time
  ;

-- task to load region
CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_REGION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.region",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/region/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_NATION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.nation",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/nation/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_SUPPLIER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.supplier",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/supplier/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_PART
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.part",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/part/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_CUSTOMER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.customer",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/customer/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_PARTSUPP
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.partsupp",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/partsupp/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_ORDERS
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.orders",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/orders/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

CREATE TASK IF NOT EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_LINEITEM
  WAREHOUSE=LOAD
  AFTER &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_START
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.&{data_product}.lineitem",
          "location": "@&{data_domain}.utils.landing/tpch-sf100/csv/lineitem/",
          "file_format": "&{data_domain}.utils.csv_fmt1",
          "infer_schema_max_file_count": 5,
          "infer_schema_max_records_per_file": 10000,
          "ignore_case": True,
          "enable_schema_evolution": True,
          "write_mode": "overwrite",
          "match_by_column_name": "case_insensitive",
          "force": True
        }
      ')
    );

-- resume the DAG tasks to enable runs
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_LINEITEM RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_ORDERS RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_PARTSUPP RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_CUSTOMER RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_PART RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_SUPPLIER RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_NATION RESUME;
ALTER TASK IF EXISTS &{data_domain}.&{data_product}.TASK_LOAD_RAW_DATA_PARALLEL_REGION RESUME;
