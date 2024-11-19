--!jinja

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 sequentially
------------------------------------------------------------------------------
{% set schemas = ["TPCH_SF100", "TPCH_SF100_ICEBERG"] %}
{% set tables = ["REGION", "NATION", "SUPPLIER", "PART", "CUSTOMER", "PARTSUPP", "ORDERS", "LINEITEM"] %}

{% for schema in schemas %}
    -- Create the root task
    CREATE OR ALTER TASK {{ schema }}.LOAD_SEQUENTIALLY_MAIN
        WAREHOUSE=LOAD
        AS
        SELECT 'dummy';

    {% for table in tables %}
        {% if loop.first %}
            CREATE OR ALTER TASK {{ schema | upper }}.LOAD_SEQUENTIALLY_{{ table | upper }}
                WAREHOUSE=LOAD
                AFTER {{ schema | upper }}.LOAD_SEQUENTIALLY_MAIN
                AS
                COPY INTO {{ schema | upper }}.{{ table | upper }}
                FROM @utils.landing/tpch-sf100/csv/{{ table | lower }}/
                FILE_FORMAT = (FORMAT_NAME = 'UTILS.CSV_FMT1')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                FORCE = TRUE;
        {% else %}
            CREATE OR ALTER TASK {{ schema | upper }}.LOAD_SEQUENTIALLY_{{ table | upper }}
                WAREHOUSE=LOAD
                AFTER {{ schema | upper }}.LOAD_SEQUENTIALLY_{{ tables[loop.index0 - 1] | upper }}
                AS
                COPY INTO {{ schema | upper }}.{{ table | upper }}
                FROM @utils.landing/tpch-sf100/csv/{{ table | lower }}/
                FILE_FORMAT = (FORMAT_NAME = 'UTILS.CSV_FMT1')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                FORCE = TRUE;

        {% endif %}
        -- Resume child tasks to enable them
        ALTER TASK IF EXISTS {{ schema | upper }}.LOAD_SEQUENTIALLY_{{ table | upper }} RESUME;
    {% endfor %}

{% endfor %}
