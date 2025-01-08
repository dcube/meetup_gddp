--!jinja

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 parallel
------------------------------------------------------------------------------
{% set tables = ["REGION", "NATION", "SUPPLIER", "PART", "CUSTOMER", "PARTSUPP", "ORDERS", "LINEITEM"] %}
{% set dag = "LOAD_PARALLEL" %}

USE ROLE SYSADMIN;

-- Create the root task
CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}
    WAREHOUSE=LOAD
    AS
    SELECT 'dummy';

-- loop over tables to creates child tasks (truncate and copy into)
{% for table in tables %}
    CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{dag}}$TRUNC_{{ table }}
        WAREHOUSE=LOAD
        AFTER {{ domain }}.{{ schema }}.{{ dag }}
        AS
        TRUNCATE TABLE {{ domain }}.{{ schema }}.{{ table }};


    -- Create tasks for loading data
    CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{dag}}$COPY_{{ table }}
        WAREHOUSE=LOAD
        AFTER {{ domain }}.{{ schema }}.{{dag}}$TRUNC_{{ table }}
        AS
        COPY INTO {{ domain }}.{{ schema }}.{{ table }}
        FROM @{{ domain }}.utils.landing/tpch-sf100/csv/{{ table | lower }}/
        FILE_FORMAT = (FORMAT_NAME = '{{ domain }}.UTILS.CSV_FMT1')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FORCE = TRUE;

    -- Resume child tasks to enable them
    ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{dag}}$TRUNC_{{ table }} RESUME;
    ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{dag}}$COPY_{{ table }} RESUME;
{% endfor %}
