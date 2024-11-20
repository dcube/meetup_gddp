--!jinja

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 sequentially
------------------------------------------------------------------------------
{% set schemas = ["TPCH_SF100", "TPCH_SF100_ICEBERG"] %}
{% set tables = ["REGION", "NATION", "SUPPLIER", "PART", "CUSTOMER", "PARTSUPP", "ORDERS", "LINEITEM"] %}

-- Loop over schemas to create tasks
{% for schema in schemas %}

    -- Create the root task
    CREATE OR ALTER TASK {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_MAIN
        WAREHOUSE=LOAD
        AS
        SELECT 'dummy';

    -- loop over tables to creates child tasks (truncate and copy into)
    {% for table in tables %}

        {% if loop.first %}
            -- first task refere to the main task
            CREATE OR ALTER TASK {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_TRUNCATE
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_MAIN
            AS
            TRUNCATE TABLE {{ domain }}.{{ schema }}.{{ table }};
        {% else %}
            -- others tasks refere to the truncate task of the previous table
            CREATE OR ALTER TASK {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_TRUNCATE
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ tables[loop.index0 - 1] }}_COPY
            AS
            TRUNCATE TABLE {{ domain }}.{{ schema }}.{{ table }};
        {% endif %}

        CREATE OR ALTER TASK {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_COPY
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_TRUNCATE
            AS
            COPY INTO {{ domain }}.{{ schema }}.{{ table }}
            FROM @{{ domain }}.utils.landing/tpch-sf100/csv/{{ table | lower }}/
            FILE_FORMAT = (FORMAT_NAME = '{{ domain }}.utils.csv_fmt1')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE;

        -- Resume child tasks to enable them
        ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_TRUNCATE RESUME;
        ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.LOAD_SEQUENTIALLY_{{ table }}_COPY RESUME;
    {% endfor %}

{% endfor %}
