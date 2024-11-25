# type: ignore
"""..."""
from typing import List, Dict
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

    @classmethod
    @st.cache_data(ttl="45m")
    def get_dag_stats(cls) -> pdDataFrame:
        """ get copy into executions from query history"""

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_tasks = (
            cls._session.sql("SELECT * FROM MEETUP_GDDP.UTILS.DAG_STATS")
            )

        return df_tasks.to_pandas()
