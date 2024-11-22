"""..."""
import altair as alt
import streamlit as st
from modules.st_utils.page_template import PageTemplate
from modules.sf_utils.dags import DagRuns


class DagsMonitorPage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()
        cls._sf_session.use_database("MEETUP_GDDP")
        cls._dags_runs = DagRuns(cls._sf_session)

    @classmethod
    def render(cls) -> None:
        """ rendering this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Complete Dags Runs</h1>",
            unsafe_allow_html=True)

        # get query history data and cache it
        if st.button("force refresh", on_click=cls._dags_runs.get_dag_runs_history.clear):
            cls._dags_runs.get_dag_runs_history.clear()

        # get the dags tasks history
        df_dags_runs_hist = cls._dags_runs.get_dag_runs_history()

        tab1, tab2 = st.tabs(["Performance", "View details"])  # type: ignore

        with tab1:
            df_dag_runs_sum = cls._dags_runs.get_dag_runs_summary()
            st.dataframe(df_dag_runs_sum)
            # define selection on click for warehouse
            # wh_nm_click = alt.selection_point(fields=["WAREHOUSE_NAME"])

            # chart to view warehouses credits by dag runs


            # chart to view dag runs durations
            chart_dags = (
                alt.Chart(df_dag_runs_sum)
                .mark_bar()  # type: ignore
                .encode(
                    x=alt.X("DAG_NAME:N", axis=alt.Axis(title=None, labelAngle=0, labelLimit=200)),
                    y=alt.Y("DAG_DURATION:Q", title=None),
                    color=alt.Color(
                        "SCHEMA_NAME:N",
                        scale=alt.Scale(domain=["TPCH_SF100", "TPCH_SF100_ICEBERG"],
                                        range=["#DC9742", "#CD4E41"]),
                        ),
                    xOffset="DAG_RUN_NUMBER:N",
                    tooltip=[
                        "DAG_RUN_NUMBER", "DAG_START_TIME", "DAG_END_TIME",
                        "DAG_DURATION", "DAG_NAME", "SCHEMA_NAME",
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
            st.altair_chart(chart_dags,  # type: ignore
                            use_container_width=True)
        with tab2:
            # view the dataframe
            st.dataframe(df_dags_runs_hist)


if __name__ == '__main__':
    # set home page properties
    my_page = DagsMonitorPage()

    # render the page
    my_page.render()
