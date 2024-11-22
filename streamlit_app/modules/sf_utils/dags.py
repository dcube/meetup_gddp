# type: ignore
"""..."""
from snowflake.snowpark.session import Session
from pandas import DataFrame as pdDataFrame
import streamlit as st


class DagRuns:
    """class get Dags runs history"""

    @classmethod
    def __init__(cls, session: Session) -> None:
        """ class constructor """
        cls._session = session

    @classmethod
    @st.cache_data(ttl="45m")
    def get_dag_runs_history(cls) -> pdDataFrame:
        """ get copy into executions from query history"""

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_tasks = (
            cls._session
            .sql("""
                SELECT
                    TH.RUN_ID,
                    DENSE_RANK() OVER (ORDER BY TH.RUN_ID) AS DAG_RUN_NUMBER,
                    MIN(TH.QUERY_START_TIME) OVER (PARTITION BY TH.RUN_ID) AS DAG_START_TIME,
                    MAX(TH.COMPLETED_TIME) OVER (PARTITION BY TH.RUN_ID) AS DAG_END_TIME,
                    DATEDIFF('S', DAG_START_TIME, DAG_END_TIME) AS DAG_DURATION_S,
                    TV.NAME AS DAG_NAME,
                    TH.ROOT_TASK_ID,
                    TH.SCHEDULED_FROM,
                    TH.NAME AS TASK_NAME,
                    SPLIT(TH.NAME,'_')[0]::STRING AS DAG_TYPE,
                    SPLIT(TH.NAME,'_')[1]::STRING AS DAG_RUN_MODE,
                    SPLIT(TH.NAME,'_')[2]::STRING AS SUBJECT,
                    SPLIT(TH.NAME,'_')[3]::STRING AS OPERATION,
                    TH.DATABASE_NAME,
                    TH.SCHEMA_NAME,
                    TH.QUERY_ID,
                    TH.QUERY_TEXT,
                    TH.QUERY_START_TIME as TASK_START_TIME,
                    TH.COMPLETED_TIME as TASK_COMPLETED_TIME,
                    TH.STATE,
                    QAH.WAREHOUSE_NAME,
                    QH.WAREHOUSE_SIZE,
                    COALESCE(CREDITS_ATTRIBUTED_COMPUTE,0)
                        + COALESCE(CREDITS_USED_QUERY_ACCELERATION,0)
                            AS TOTAL_CREDITS,
                    QH.COMPILATION_TIME/1000 AS COMPILATION_TIME_S,
                    QH.QUEUED_PROVISIONING_TIME/1000 AS QUEUED_PROVISIONING_TIME_S,
                    QH.QUEUED_OVERLOAD_TIME/1000 AS QUEUED_OVERLOAD_TIME_S,
                    QH.TOTAL_ELAPSED_TIME/1000 AS TOTAL_ELAPSED_TIME_S,
                    QH.PARTITIONS_SCANNED,
                    QH.PARTITIONS_TOTAL,
                    QH.BYTES_SPILLED_TO_LOCAL_STORAGE,
                    QH.BYTES_SPILLED_TO_REMOTE_STORAGE,
                    QH.ROWS_PRODUCED,
                    QH.ROWS_INSERTED,
                    QH.ROWS_UPDATED,
                    QH.ROWS_DELETED,
                    QH.ROWS_UNLOADED,
                    QH.ROWS_WRITTEN_TO_RESULT
                FROM
                    SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY AS TH
                    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.TASK_VERSIONS AS TV
                        ON TV.ID = TH.ROOT_TASK_ID
                        AND TV.GRAPH_VERSION = TH.GRAPH_VERSION
                    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY AS QAH
                        ON QAH.QUERY_ID = TH.QUERY_ID
                    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS QH
                        ON QH.QUERY_ID = TH.QUERY_ID
                WHERE
                    TH.DATABASE_NAME='MEETUP_GDDP'
                AND TH.QUERY_START_TIME >= '2024-11-22 02:44:50'
                ORDER BY TH.RUN_ID, DAG_START_TIME, TH.QUERY_START_TIME
                """)
            )

        return df_tasks.to_pandas()

    def get_dag_runs_summary(cls) -> pdDataFrame:
        pd_df_dag_runs = cls.get_dag_runs_history()

        pd_df_dag_runs_agg = (
            pd_df_dag_runs
            .groupby(
                [
                    "DAG_RUN_NUMBER", "DAG_START_TIME", "DAG_END_TIME",
                    "DAG_DURATION_S", "DAG_NAME", "SCHEMA_NAME",
                    "DAG_TYPE", "DAG_RUN_MODE",
                    "WAREHOUSE_NAME", "WAREHOUSE_SIZE"
                ],
                as_index=False
            )
            .agg(
                {
                    "TOTAL_CREDITS": "sum",
                    "COMPILATION_TIME_S": "sum",
                    "QUEUED_PROVISIONING_TIME_S": "sum",
                    "QUEUED_OVERLOAD_TIME_S": "sum",
                    "TOTAL_ELAPSED_TIME_S": "sum",
                    "PARTITIONS_SCANNED": "sum",
                    "PARTITIONS_TOTAL": "sum",
                    "BYTES_SPILLED_TO_LOCAL_STORAGE": "sum",
                    "BYTES_SPILLED_TO_REMOTE_STORAGE": "sum",
                    "ROWS_PRODUCED": "sum",
                    "ROWS_INSERTED": "sum",
                    "ROWS_UPDATED": "sum",
                    "ROWS_DELETED": "sum",
                    "ROWS_UNLOADED": "sum",
                    "ROWS_WRITTEN_TO_RESULT": "sum",
                }
            )
        )

        return pd_df_dag_runs_agg

    def get_dag_runs_warehouses_costs(cls) -> pdDataFrame:
        pd_df_dag_runs = cls.get_dag_runs_history()

        pd_df_dag_runs_agg = (
            pd_df_dag_runs
            .groupby(
                [
                    "WAREHOUSE_NAME", "WAREHOUSE_SIZE"
                ],
                as_index=False
            )
            .agg(
                {
                    "TOTAL_CREDITS": "sum"
                }
            )
        )

        return pd_df_dag_runs_agg
