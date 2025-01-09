--!jinja

------------------------------------------------------------------------------
-- create the DAG to exec tpch queries in parallel
------------------------------------------------------------------------------
{% set dag = "NLITX_PARALLEL" %}

USE ROLE SYSADMIN;

-- Create the root task
CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}
    WAREHOUSE=ANALYSIS
    SCHEDULE = '{{ schedule }}'
    AS
    SELECT 'dummy';

-- loop over index to create the 22 children tasks to exec the tpch queries
{% for query_idx in range(1, 23) %}
    -- Set filename
    {% set basename = 'tpch_' + '%02d' % query_idx  %}

    -- Create tasks for loading data
    CREATE OR ALTER TASK {{ domain }}.{{ schema }}.{{ dag }}$SLECT_{{ basename | upper }}
        WAREHOUSE=ANALYSIS
        AFTER {{ domain }}.{{ schema }}.{{ dag }}
        AS
        EXECUTE IMMEDIATE FROM @{{ domain }}.UTILS.GIT_REPO/{{ git_ref }}/snowcli/tpch_queries/{{ basename | lower }}.sql
        ;

    -- Resume child tasks to enable them
    ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{ dag }}$SLECT_{{ basename | upper }} RESUME;
    ALTER TASK IF EXISTS {{ domain }}.{{ schema }}.{{dag}} RESUME;
{% endfor %}
