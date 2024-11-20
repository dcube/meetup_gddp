"""..."""
from pandas import DataFrame as pdDataFrame, to_datetime
import altair as alt
import streamlit as st
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import col, datediff, lit, current_date, dateadd
from modules.st_utils.page_template import PageTemplate


class CompleteDagsPage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()
        cls._sf_session.use_database("MEETUP_GDDP")

    @classmethod
    @st.cache_data(ttl="45m")
    def get_tasks_history(cls,
                         timeframe_option: str = "day",
                         timeframe_depth_option: int = 30
                         ) -> pdDataFrame:
        """ get copy into executions from query history"""

        # get recent dags from information schema complete_task_graph table function
        # this table function return dags completed during the last 60 minutes
        df_tasks = (
            cls._sf_session
            .sql("""
                 select
                    th.run_id,
                    dense_rank() over (order by th.run_id) as dag_run_number,
                    min(th.query_start_time) over (partition by th.run_id) as dag_start_time,
                    max(th.completed_time) over (partition by th.run_id) as dag_end_time,
                    datediff('s', dag_start_time, dag_end_time) as dag_duration,
                    first_value(th.name) over (partition by th.run_id order by th.query_start_time) as dag_root_task,
                    th.root_task_id,
                    th.scheduled_from,
                    th.name,
                    split(th.name,'_')[0]::string as task_type,
                    split(th.name,'_')[1]::string as run_mode,
                    split(th.name,'_')[2]::string as subject,
                    th.database_name,
                    th.schema_name,
                    th.query_id,
                    th.query_text,
                    th.query_start_time,
                    th.completed_time,
                    th.state,
                    qah.warehouse_name,
                    qh.warehouse_size,
                    coalesce(credits_attributed_compute,0)
                        + coalesce(credits_used_query_acceleration,0)
                            as total_credits,
                    qh.compilation_time,
                    qh.queued_provisioning_time,
                    qh.total_elapsed_time,
                    qh.partitions_scanned,
                    qh.partitions_total,
                    qh.bytes_spilled_to_local_storage,
                    qh.bytes_spilled_to_remote_storage,
                    qh.rows_produced,
                    qh.rows_inserted,
                    qh.rows_updated,
                    qh.rows_deleted,
                    qh.rows_unloaded,
                    qh.rows_written_to_result
                from
                    snowflake.account_usage.task_history as th
                    left join snowflake.account_usage.query_attribution_history as qah
                        on qah.query_id = th.query_id
                    inner join snowflake.account_usage.query_history as qh
                        on qh.query_id = th.query_id
                where
                    th.database_name='MEETUP_GDDP'
                and th.query_start_time > current_date() - 7
                 """)
            )

        return df_tasks.to_pandas()

    @classmethod
    def render(cls) -> None:
        """ rendering this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Complete Dags Runs</h1>",
            unsafe_allow_html=True)

        # get query history data and cache it
        if st.button("force refresh", on_click=cls.get_tasks_history.clear):
            cls.get_tasks_history.clear()

        # get the tasks history
        df_tasks = cls.get_tasks_history()

        tab1, tab2 = st.tabs(["Performance", "View details"])  # type: ignore

        with tab1:
            # define selection on click for table_name
            # aggregate the dataframe to get dags summary
            df_dags = (
                df_tasks.groupby(
                       ["DAG_RUN_NUMBER", "DAG_START_TIME", "DAG_END_TIME", "DAG_DURATION", "DAG_ROOT_TASK", "SCHEMA_NAME"],
                       as_index=False
                    )
                .agg(
                       {
                            "TOTAL_CREDITS": "sum",
                            "COMPILATION_TIME": "sum",
                            "QUEUED_PROVISIONING_TIME": "sum",
                            "TOTAL_ELAPSED_TIME": "sum",
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

            chart_part1 = (
                alt.Chart(df_dags).mark_bar()  # type: ignore
                .encode(
                    x=alt.X("DAG_ROOT_TASK:N", axis=alt.Axis(title=None)),
                    y=alt.Y("DAG_DURATION:Q", title=None),
                    color=alt.Color(
                        "SCHEMA_NAME:N",
                        scale=alt.Scale(domain=["TPCH_SF100", "TPCH_SF100_ICEBERG"],
                                        range=["#DC9742", "#CD4E41"]),
                        ),
                    xOffset="DAG_RUN_NUMBER:N",
                    tooltip=[
                        "DAG_RUN_NUMBER", "DAG_START_TIME", "DAG_END_TIME",
                        "DAG_DURATION", "DAG_ROOT_TASK", "SCHEMA_NAME",
                        "TOTAL_CREDITS", "COMPILATION_TIME", "QUEUED_PROVISIONING_TIME",
                        "TOTAL_ELAPSED_TIME", "PARTITIONS_SCANNED", "PARTITIONS_TOTAL",
                        "BYTES_SPILLED_TO_LOCAL_STORAGE", "BYTES_SPILLED_TO_REMOTE_STORAGE",
                        "ROWS_PRODUCED", "ROWS_INSERTED", "ROWS_UPDATED", "ROWS_DELETED",
                        "ROWS_UNLOADED", "ROWS_WRITTEN_TO_RESULT"
                        ]
                )
                # .transform_filter(tbl_click)
                .properties(width=600, height=250,
                            title="Dag duration (sec) by run")
            )

            # Display the chart in Streamlit
            st.altair_chart(chart_part1,  # type: ignore
                            use_container_width=True)
        with tab2:
            # view the dataframe
            st.dataframe(df_tasks)


if __name__ == '__main__':
    # set home page properties
    my_page = CompleteDagsPage()

    # render the page
    my_page.render()
