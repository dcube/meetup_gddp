"""..."""
from pandas import DataFrame as pdDataFrame
import altair as alt
import streamlit as st
from snowflake.snowpark.functions import col, datediff, lit, current_date, dateadd
from modules.st_utils.page_template import PageTemplate


class CompleteDagsPage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()
        cls._sf_session.use_database("MEETUP_GDDP")

    @staticmethod
    def timeframe_to_seconds(timeframe_option: str,
                             timeframe_depth_option: int) -> int:
        """convert a timeframe into seconds duration"""
        # default return value
        duration_seconds = 0

        # Dictionary to map each timeframe to its equivalent seconds
        timeframe_to_seconds = {
            "second": 1,
            "minute": 60,
            "hour": 3600,
            "day": 86400,       # 24 hours * 60 minutes * 60 seconds
            "week": 604800,     # 7 days * 86400 seconds
            "month": 2592000,   # Approx. 30 days * 86400 seconds
            "year": 31536000    # Approx. 365 days * 86400 seconds
        }

        if timeframe_option in timeframe_to_seconds:
            duration_seconds = timeframe_to_seconds[timeframe_option] * timeframe_depth_option

        return duration_seconds

    @classmethod
    @st.cache_data(ttl="1d")
    def get_dags_history(cls,
                         timeframe_option: str = "day",
                         timeframe_depth_option: int = 30
                         ) -> pdDataFrame:
        """ get copy into executions from query history"""
        timeframe_seconds = cls.timeframe_to_seconds(timeframe_option, timeframe_depth_option)
        timeframe_seconds_1 = 3660 if timeframe_seconds >= 3660 else timeframe_seconds
        timeframe_seconds_2 = timeframe_seconds - 3660 if timeframe_seconds > 3600 else 0

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_dags = (
            cls._sf_session
            .table_function("INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS")
            .filter(
                col("COMPLETED_TIME") >=
                    dateadd("second", lit(-timeframe_seconds_1), lit(current_date()))
                )
            .with_column(
                "DURATION",
                datediff(
                    "second",
                    col("QUERY_START_TIME"),
                    col("COMPLETED_TIME"))
                )
            .select("RUN_ID", "GRAPH_RUN_GROUP_ID", "ROOT_TASK_ID",
                    "SCHEMA_NAME", "ROOT_TASK_NAME", "STATE",
                    "QUERY_START_TIME", "DURATION")
            )

        if timeframe_seconds_2 > 0:
            # get dags from the "SNOWFLAKE.ACCOUNT_USAGE.COMPLETE_TASK_GRAPHS"
            # latency 45 min
            df_old_dags = (
                cls._sf_session
                .table("SNOWFLAKE.ACCOUNT_USAGE.COMPLETE_TASK_GRAPHS") \
                .filter(
                    ( col("DATABASE_NAME") == lit("MEETUP_GDDP") )
                    & (
                        col("COMPLETED_TIME") >=
                        dateadd("second", lit(-timeframe_seconds_2), lit(current_date()))
                    )
                    & (
                        col("COMPLETED_TIME") <
                        dateadd("second", lit(-3600), lit(current_date()) )
                    )
                    )
                .with_column(
                    "DURATION",
                    datediff(
                        "second",
                        col("QUERY_START_TIME"),
                        col("COMPLETED_TIME"))
                    )
                .select(
                    "RUN_ID", "GRAPH_RUN_GROUP_ID", "ROOT_TASK_ID",
                    "SCHEMA_NAME", "ROOT_TASK_NAME", "STATE",
                    "QUERY_START_TIME", "DURATION"
                    )
                )

            df_dags = df_dags.union_by_name(df_old_dags)

        return df_dags.to_pandas()

    @classmethod
    def render(cls) -> None:
        """ rendering this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Complete Dags Runs</h1>",
            unsafe_allow_html=True)

        # get query history data and cache it
        st.write(cls.get_dags_history())


if __name__ == '__main__':
    # set home page properties
    my_page = CompleteDagsPage()

    # render the page
    my_page.render()
