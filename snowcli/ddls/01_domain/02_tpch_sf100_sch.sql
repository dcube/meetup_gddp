USE ROLE SYSADMIN;

USE DATABASE &{domain};

------------------------------------------------------------------------------
-- Create the data products TPCH_SF100
------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS TPCH_SF100;

  -- create technical roles
  USE ROLE SECURITYADMIN;
  -- Technical role for owners on schema TPCH_SF100
  CREATE ROLE IF NOT EXISTS TR_&{domain}_TPCH_SF100_OWNR;
    -- defining privileges
    EXECUTE IMMEDIATE
    $$
    BEGIN
      GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_OWNR;
      GRANT OWNERSHIP ON FUTURE DYNAMIC TABLES IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_OWNR;
      GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_OWNR;
      GRANT OWNERSHIP ON FUTURE TASKS IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_OWNR;
    EXCEPTION
      WHEN STATEMENT_ERROR THEN
            RETURN OBJECT_CONSTRUCT('EXCEPTION', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate,
                            'TYPE', 'warning');
        WHEN OTHER THEN
            RAISE;
    END;
    $$;
    -- inheritance to functional roles
    GRANT ROLE TR_&{domain}_TPCH_SF100_OWNR TO ROLE FR_&{domain}_OPS;

  -- Technical role for reader on schema &{domain}.TPCH_SF100
  CREATE ROLE IF NOT EXISTS TR_&{domain}_TPCH_SF100_READR;
    -- defining privileges
    EXECUTE IMMEDIATE
    $$
    BEGIN
      GRANT SELECT ON FUTURE TABLES IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_READR;
      GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_READR;
      GRANT SELECT ON FUTURE VIEWS IN SCHEMA &{domain}.TPCH_SF100 TO ROLE TR_&{domain}_TPCH_SF100_READR;
    EXCEPTION
      WHEN STATEMENT_ERROR THEN
            RETURN OBJECT_CONSTRUCT('EXCEPTION', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate,
                            'TYPE', 'warning');
        WHEN OTHER THEN
            RAISE;
    END;
    $$;
    -- inheritance to functional roles
    GRANT ROLE TR_&{domain}_TPCH_SF100_READR TO ROLE FR_&{domain}_ANALYSTS;

  USE ROLE SYSADMIN;

  -- Deploy tpch tables
  EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/tables/tables.sql
  USING (domain => '&{domain}', schema => 'TPCH_SF100')
  DRY_RUN = &{dry_run};

  -- Deploy tpch dags
  -- load dag
  EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/dags/dag_load_parallel.sql
  USING (domain => '&{domain}', schema => 'TPCH_SF100')
  DRY_RUN = &{dry_run};

  -- analytics dags
  EXECUTE IMMEDIATE FROM @&{domain}.UTILS.GIT_REPO/&{git_ref}/snowcli/tpch_ddl_templates/dags/dag_nlitx_parallel.sql
  USING (domain => '&{domain}', schema => 'TPCH_SF100', git_ref => '&{git_ref}')
  DRY_RUN = &{dry_run};
