# type: ignore
"""..."""
from snowflake.snowpark.session import Session
from pandas import DataFrame as pdDataFrame
import streamlit as st
from sf_utils.pricing import Pricing


class DagRuns:
    """class get Dags runs history"""

    @classmethod
    def __init__(cls, session: Session, provider_region: str) -> None:
        """ class constructor """
        cls._session = session
        cls._provider_region = provider_region
        cls._cost_standard = Pricing[f"STANDARD_{provider_region}"].value
        cls._cost_enterprise = Pricing[f"ENTERPRISE_{provider_region}"].value
        cls._cost_businesscritical = Pricing[f"BUSINESSCRITICAL_{provider_region}"].value

    @classmethod
    # @st.cache_data(ttl="1d")
    def get_dag_run_stats(cls) -> pdDataFrame:
        """ get dags run history metrics """

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_dags = cls._session.table("meetup_gddp.utils.dag_run_stats").to_pandas()

        df_dags["DAG_FORMAT"] = (df_dags["DAG_NAME"] + " " + df_dags["DATA_FORMAT"])
        df_dags["COST_STANDARD"] = (df_dags["DAG_CREDITS"] * cls._cost_standard)
        df_dags["COST_ENTERPRISE"] = (df_dags["DAG_CREDITS"] * cls._cost_enterprise)
        df_dags["COST_BUSINESS_CRITICAL"] = (df_dags["DAG_CREDITS"] * cls._cost_businesscritical)

        return df_dags

    @classmethod
    @st.cache_data(ttl="1d")
    def get_dag_tasks_stats(cls) -> pdDataFrame:
        """ get dags run history metrics detailed by tasks"""

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_tasks = (
            cls._session.sql(
                """
                select
                    drm.run_id,
                    drm.dag_run_number,
                    drm.dag_name,
                    drm.workload_type,
                    drm.run_mode,
                    drm.data_format,
                    drm.warehouse_size,
                    drm.task_subname,
                    drm.total_credits,
                    drm.compilation_time_s,
                    drm.queued_provisioning_time_s,
                    drm.queued_overload_time_s,
                    drm.total_elapsed_time_s,
                    drm.total_elapsed_time_s
                        - drm.compilation_time_s
                        - drm.queued_provisioning_time_s
                        - drm.queued_overload_time_s
                        as execution_time_s,
                    drm.partitions_scanned,
                    drm.partitions_total,
                    drm.rows_produced
                from
                    meetup_gddp.utils.dag_run_monitor drm
                where
                    drm.total_credits > 0
                """)
            ).to_pandas()

        df_tasks["COST_STANDARD"] = (df_tasks["TOTAL_CREDITS"] * cls._cost_standard)
        df_tasks["COST_ENTERPRISE"] = (df_tasks["TOTAL_CREDITS"] * cls._cost_enterprise)
        df_tasks["COST_BUSINESS_CRITICAL"] = (df_tasks["TOTAL_CREDITS"] * cls._cost_businesscritical)

        return df_tasks
