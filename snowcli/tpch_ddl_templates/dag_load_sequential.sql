--!jinja

------------------------------------------------------------------------------
-- create the DAG to ingest data (batch mode) into TPCH_SF100 sequentially
------------------------------------------------------------------------------
{% set schemas = ["TPCH_SF100", "TPCH_SF100_ICEBERG"] %}
{% set tables = ["REGION", "NATION", "SUPPLIER", "PART", "CUSTOMER", "PARTSUPP", "ORDERS", "LINEITEM"] %}
{M set dag = "LOAD_SEQUENTIAL"}

-- Loop over schemas to create tasks
{% for schema in schemas %}

    -- Create the root task
    CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}
        WAREHOUSE=LOAD
        AS
        SELECT 'dummy';

    -- loop over tables to creates child tasks (truncate and copy into)
    {% for table in tables %}

        {% if loop.first %}
            -- first task refere to the main task
            CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}$TRUNC_{{ table }}
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.{{ dag }}
            AS
            TRUNCATE TABLE {{ domain }}.{{ schema }}.{{ table }};
        {% else %}
            -- others tasks refere to the truncate task of the previous table
            CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}$TRUNC_{{ table }}
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.{{ dag }}$COPY_{{ tables[loop.index0 - 1] }}
            AS
            TRUNCATE TABLE {{ domain }}.{{ schema }}.{{ table }};
        {% endif %}

        CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}$COPY_{{ table }}
            WAREHOUSE=LOAD
            AFTER {{ domain }}.{{ schema }}.{{ dag }}$TRUNC_{{ table }}
            AS
            COPY INTO {{ domain }}.{{ schema }}.{{ table }}
            FROM @{{ domain }}.utils.landing/tpch-sf100/csv/{{ table | lower }}/
            FILE_FORMAT = (FORMAT_NAME = '{{ domain }}.utils.csv_fmt1')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE;

        -- Resume child tasks to enable them
        ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{ dag }}$TRUNC_{{ table }} RESUME;
        ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{ dag }}$COPY_{{ table }} RESUME;
    {% endfor %}

{% endfor %}
