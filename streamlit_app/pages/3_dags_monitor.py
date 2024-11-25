"""..."""
import os
from pandas import DataFrame as PdDataFrame
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
            # get dag run stats
            pd_dag_stats_dset = cls._dags_runs.get_dag_stats()

            (col_fltr1, col_fltr2, col_fltr3, col_fltr4) = st.columns(4)
            with col_fltr1:
                st.multiselect("Select a workload",
                            options=pd_dag_stats_dset["WORKLOAD_TYPE"].unique(),
                            key=f"{os.path.basename(__file__)}._fltr_workload_type")

            with col_fltr2:
                st.multiselect("Select a run mode",
                            options=pd_dag_stats_dset["RUN_MODE"].unique(),
                            key=f"{os.path.basename(__file__)}._fltr_run_mode")

            with col_fltr3:
                st.multiselect("Select a warehouse size",
                            options=pd_dag_stats_dset["WAREHOUSE_SIZE"].unique(),
                            key=f"{os.path.basename(__file__)}._fltr_wh_size")

            pd_dag_stats = pd_dag_stats_dset
            _fltr_workload_type = st.session_state.get(f"{os.path.basename(__file__)}._fltr_workload_type")
            if _fltr_workload_type:
                pd_dag_stats = pd_dag_stats[pd_dag_stats["WORKLOAD_TYPE"].isin(_fltr_workload_type)]

            _fltr_run_mode = st.session_state.get(f"{os.path.basename(__file__)}._fltr_run_mode")
            if _fltr_run_mode:
                pd_dag_stats = pd_dag_stats[pd_dag_stats["RUN_MODE"].isin(_fltr_run_mode)]

            _fltr_wh_size = st.session_state.get(f"{os.path.basename(__file__)}._fltr_wh_size")
            if _fltr_wh_size:
                pd_dag_stats = pd_dag_stats[pd_dag_stats["WAREHOUSE_SIZE"].isin(_fltr_wh_size)]

            # Define selection on click for DAG_NAME, WAREHOUSE_SIZE, and DATA_FORMAT
            selection = alt.selection_point(fields=["DAG_NAME", "WAREHOUSE_SIZE", "DATA_FORMAT"], on="click")

            # chart dag runs stats
            chart_dags = (
                alt.Chart(pd_dag_stats)
                .mark_bar()  # type: ignore
                .encode(
                    x=alt.X("DAG_NAME:N", title=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("AVG_DURATION_S:Q", title=None),
                    xOffset=alt.XOffset("WAREHOUSE_SIZE:N", sort=["Medium", "Large", "X-Large"]),
                    row=alt.Row("DATA_FORMAT"),
                    color=alt.condition(
                        selection,
                        alt.Color(
                            "WAREHOUSE_SIZE:N",
                            title="Warehouse size",
                            scale=alt.Scale(
                                domain=["Medium", "Large", "X-Large"],
                                range=["#C6D63B", "#DC9742", "#CD4E41"]
                                )
                            ),
                        alt.value("lightgray")  # Gray color for unselected bars
                        )

                )
                .properties(height=200, title="Dag duration (sec) by run")
                .add_params(selection)
            )

            # display the chart
            dag_chart = st.altair_chart(chart_dags, use_container_width=True, on_select="rerun")

            # create data frame with selected points on dag_chart
            pd_dag_chart_sel = PdDataFrame(PdDataFrame(dag_chart.selection)["param_1"].to_list())

            # filtering the dag stats rows depending on chart selection
            pd_dag_stats_selection = pd_dag_stats
            if len(pd_dag_chart_sel) > 0:
                # st.write(pd_dag_chart_sel)
                # Apply filter based on selection
                pd_dag_stats_selection = pd_dag_stats_selection.merge(
                    pd_dag_chart_sel,
                    on=["DAG_NAME", "DATA_FORMAT", "WAREHOUSE_SIZE"],
                    how="inner")

            # display th dag stats rows details
            st.dataframe(pd_dag_stats_selection)

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
