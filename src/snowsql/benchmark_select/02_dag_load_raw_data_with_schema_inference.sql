------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100
-- with schema inference
------------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- create the sequential dag to load raw data sequentially
CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  WAREHOUSE=LOAD
  AS
  SELECT 'dummy'
  ;

-- task to load region
CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_REGION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.region",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_NATION
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.nation",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_SUPPLIER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.supplier",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_PART
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.part",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_CUSTOMER
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.customer",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_PARTSUPP
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.partsupp",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ORDERS
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.orders",
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

CREATE TASK IF NOT EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_LINEITEM
  WAREHOUSE=LOAD
  AFTER &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ENTRYPOINT
  AS
  CALL &{data_domain}.utils.LOAD_RAW_DATA_CSV(
      parse_json('
        {
          "table_name": "&{data_domain}.TPCH_SF100.lineitem",
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
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_LINEITEM RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_ORDERS RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_PARTSUPP RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_CUSTOMER RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_PART RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_SUPPLIER RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_NATION RESUME;
ALTER TASK IF EXISTS &{data_domain}.TPCH_SF100.LOAD_RAW_DATA_WITH_SCHEMA_INFERENCE_REGION RESUME;
