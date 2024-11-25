"""..."""
import os
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
        cls._dags_runs = DagRuns(cls._sf_session, "AWS_EU_PARIS")

    @classmethod
    def display_tab_dag_runs(cls) -> None:
        """ displaying the """
        with cls._tab_dag_runs:
            # df_dag_runs_sum = cls._dags_runs.get_dag_runs_summary()
            # define selection on click for warehouse

            # wh_nm_click = alt.selection_point(fields=["WAREHOUSE_NAME"])
            pd_dag_stats = cls._dags_runs.get_dag_stats()

            # radio button to switch between parallel and sequential runs
            fltr_dag_name = st.selectbox(
                "Select a Dag",
                options=pd_dag_stats["DAG_NAME"].unique(),
                key=f"{os.path.basename(__file__)}._fltr_dag_name"
                )

            chart_dags = (
                alt.Chart(pd_dag_stats)
                .mark_bar()  # type: ignore
                .encode(
                    x=alt.X("DAG_NAME:N",
                            title=None,
                            axis=alt.Axis(
                                labelAngle=0,
                                labelExpr="replace(datum.value, '\\\\n', '\\n')"
                                )
                            ),
                    y=alt.Y("AVG_DURATION_S:Q", title=None),
                    xOffset=alt.XOffset("WAREHOUSE_SIZE:N", sort=["Medium", "Large", "X-Large"]),
                    row=alt.Row("DATA_FORMAT"),
                    color=alt.Color(
                        "WAREHOUSE_SIZE:N",
                        title="Warehouse size",
                        scale=alt.Scale(
                            domain=["Medium", "Large", "X-Large"],
                            range=["#C6D63B", "#DC9742", "#CD4E41"]
                            )
                        )

                )

                .transform_filter(alt.selection_point(fields=[fltr_dag_name]))
                .properties(width=600, height=200,
                            title="Dag duration (sec) by run")
            )

            st.altair_chart(chart_dags, use_container_width=False)

            st.dataframe(pd_dag_stats)

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Complete Dags Runs</h1>",
            unsafe_allow_html=True)

        # define the tabs
        cls._tab_dag_runs, cls._tab_tasks = st.tabs(["Dag runs", "View tasks details"])  # type: ignore

        # display the tab dag runs
        cls.display_tab_dag_runs()


if __name__ == "__main__":
    # set home page properties
    my_page = DagsMonitorPage()

    # display the page
    my_page.display()
