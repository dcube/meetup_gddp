USE WAREHOUSE MANAGE;

-- Git fetch
ALTER GIT REPOSITORY MEETUP_GDDP.UTILS.GIT_REPO FETCH;

--view files on repo
ls @MEETUP_GDDP.UTILS.GIT_REPO/branches/"feature/setting_up_project"/;

-- Deploy dag jinja templating from git integration
EXECUTE IMMEDIATE FROM @MEETUP_GDDP.UTILS.GIT_REPO/branches/"feature/setting_up_project"/snowcli/tpch_ddl_templates/dags/dag_load_sequential.sql
USING (domain => 'MEETUP_GDDP')
DRY_RUN = TRUE;

-- Change the warehouse size
ALTER WAREHOUSE LOAD SET WAREHOUSE_SIZE='X-LARGE';

-- show tasks
SHOW TASKS IN SCHEMA MEETUP_GDDP.TPCH_SF100;

-- Execute the root task
EXECUTE TASK MEETUP_GDDP.TPCH_SF100.LOAD_PARALLEL;
