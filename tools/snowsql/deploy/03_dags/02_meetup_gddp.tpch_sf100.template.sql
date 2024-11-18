------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 parallel
------------------------------------------------------------------------------
-- root task
-- create the sequential dag to load raw data sequentially
CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
    WAREHOUSE=LOAD
    AS
    SELECT 'dummy'
    ;

    -- task to load region
    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_REGION
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.region
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/region/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_NATION
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.nation
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/nation/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_SUPPLIER
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.supplier
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/supplier/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PART
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.part
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/part/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True
        ;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_CUSTOMER
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.customer
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/customer/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True
        ;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PARTSUPP
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.partsupp
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/partsupp/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_ORDERS
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.orders
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/orders/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True
        ;

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_LINEITEM
        WAREHOUSE=LOAD
        AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL
        AS
        copy into MEETUP_GDDP.TPCH_SF100.lineitem
            from @MEETUP_GDDP.utils.landing/tpch-sf100/csv/lineitem/
            file_format = (format_name = 'MEETUP_GDDP.UTILS.CSV_FMT1')
            match_by_column_name = case_insensitive
            force = True
        ;

    -- resume the DAG tasks to enable runs
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_LINEITEM RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_ORDERS RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_PARTSUPP RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_CUSTOMER RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_PART RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_SUPPLIER RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_NATION RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_REGION RESUME;


------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 parallel
-- using snowpark generic stored proc
------------------------------------------------------------------------------
CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK WAREHOUSE=LOAD AS
    SELECT 'dummy'
    ;

    -- task to load region
    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_REGION WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.REGION", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/region/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_NATION WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.NATION", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/nation/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_SUPPLIER WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.SUPPLIER", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/supplier/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PART WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.PART", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/part/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_CUSTOMER WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.CUSTOMER", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/customer/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_PARTSUPP WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.PARTSUPP", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/partsupp/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_ORDERS WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.ORDERS", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/orders/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    CREATE OR ALTER TASK MEETUP_GDDP.TPCH_SF100.LOAD_LINEITEM WAREHOUSE=LOAD AFTER MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL_SNOWPARK AS
        CALL MEETUP_GDDP.UTILS.LOAD_FROM_CSV(parse_json('{ "table_name": "MEETUP_GDDP.TPCH_SF100.LINEITEM", "stage_path": "@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/lineitem/", "file_format": "MEETUP_GDDP.UTILS.CSV_FMT1", "mode": "truncate", "force": true}'));

    -- resume the DAG tasks to enable runs
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_LINEITEM_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_ORDERS_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_PARTSUPP_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_CUSTOMER_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_PART_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_SUPPLIER_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_NATION_SNOWPARK RESUME;
    ALTER TASK IF EXISTS MEETUP_GDDP.TPCH_SF100.LOAD_REGION_SNOWPARK RESUME;
