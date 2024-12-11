"""..."""
import os
from pandas import DataFrame as PdDataFrame
import altair as alt
import streamlit as st
from modules.st_utils.page_template import PageTemplate
from modules.sf_utils.dags import DagRuns

class TasksMonitorPage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()

        # init dags runs
        cls._tasks_runs = DagRuns(cls._sf_session, "AWS_EU_PARIS")

        cls._dataset = cls._tasks_runs.get_dag_tasks_stats()

        # dropdown definitions for sidebar
        cls._sidebar_filters = [
            {"col": "WORKLOAD_TYPE", "key": f"{os.path.basename(__file__)}._fltr_workload_type", "label": "Select workload", "type": "radio"},
            {"col": "RUN_MODE", "key": f"{os.path.basename(__file__)}._fltr_run_mode", "label": "Select run mode", "type": "radio"},
            {"col": "DATA_FORMAT", "key": f"{os.path.basename(__file__)}._fltr_dta_fmt", "label": "Select data format", "type": "radio"},
            {"col": "WAREHOUSE_SIZE", "key": f"{os.path.basename(__file__)}._fltr_wh_size", "label": "Select warehouse size", "type": "multiselect"}
        ]

        # default session state for chart metric and agg fx
        if not st.session_state.get(f"{os.path.basename(__file__)}._sel_metric"):
            st.session_state[f"{os.path.basename(__file__)}._sel_metric"] = "TOTAL_ELAPSED_TIME_S"
        if not st.session_state.get(f"{os.path.basename(__file__)}._sel_agg_fx"):
            st.session_state[f"{os.path.basename(__file__)}._sel_agg_fx"] = "mean"

    @classmethod
    def display_sidebar(cls) -> None:
        """ display sidebar"""
        # loop over dropdown def in _sidebar_filters
        cols = st.columns(len(cls._sidebar_filters))
        for idx in range(0, len(cls._sidebar_filters)):
            # display filters on sidebar
            with cols[idx]:
                if cls._sidebar_filters[idx]["type"] == "radio":
                    st.sidebar.radio(
                        cls._sidebar_filters[idx]["label"],
                        options=cls._dataset[cls._sidebar_filters[idx]["col"]].unique(),
                        key=cls._sidebar_filters[idx]["key"]
                        )
                elif cls._sidebar_filters[idx]["type"] == "multiselect":
                    st.sidebar.multiselect(
                        cls._sidebar_filters[idx]["label"],
                        options=cls._dataset[cls._sidebar_filters[idx]["col"]].unique(),
                        key=cls._sidebar_filters[idx]["key"])

    @classmethod
    def apply_filters(cls, sidebar: bool = True,
                      chart_selection: bool = False) -> PdDataFrame:
        """ apply filters onto the dataset """
        df_filtr = cls._dataset

        if sidebar:
            # apply sidebar filters
            for idx in range(0, len(cls._sidebar_filters)):
                selected_values = st.session_state.get(cls._sidebar_filters[idx]["key"])
                if selected_values:
                    if isinstance(st.session_state.get(cls._sidebar_filters[idx]["key"]), list):
                        df_filtr = df_filtr[df_filtr[cls._sidebar_filters[idx]["col"]].isin(selected_values)]
                    else:
                        df_filtr = df_filtr[df_filtr[cls._sidebar_filters[idx]["col"]]==selected_values]

        #if chart_selection:
        #    # apply chart selection filters
        #    df_chart_sel = PdDataFrame(PdDataFrame(cls._chart_state.selection)["param_1"].to_list())
        #
        #    if len(df_chart_sel) > 0:
        #        df_filtr = df_filtr.merge(
        #            df_chart_sel,
        #            on=["TASK_SUBNAME", "WAREHOUSE_SIZE"],
        #            how="inner")

        return df_filtr

    @classmethod
    def display_as_chart(cls) -> None:
        """ Chart the dataset filtered """
        # apply the filters (sidebar only)
        df_fltrd = cls.apply_filters(True, False)

        # Define selection on click for TASK_SUBNAME and WAREHOUSE_SIZE
        cls._selection = alt.selection_point(fields=["TASK_SUBNAME", "WAREHOUSE_SIZE"], on="click", clear="dblclick")

        # Selector to choose the metric and the aggregate function to chart
        cols_kpi_selector = st.columns([0.2, 0.2, 0.6])
        metric = cols_kpi_selector[0].selectbox(
            label="Choose the metric:",
            options=["TOTAL_ELAPSED_TIME_S", "EXECUTION_TIME_S", "QUEUED_OVERLOAD_TIME_S", "COMPILATION_TIME_S",
                    "TOTAL_CREDITS", "COST_STANDARD", "COST_ENTERPRISE", "COST_BUSINESS_CRITICAL"],
            key=f"{os.path.basename(__file__)}._sel_metric"
        )
        agg_fx = cols_kpi_selector[1].selectbox(
            label="Choose the aggregate function:",
            options=["mean", "min", "max", "sum", "std"],
            key=f"{os.path.basename(__file__)}._sel_agg_fx"
        )

        # Create the bar chart
        chart = (
            alt.Chart(df_fltrd)
            .mark_bar()
            .encode(
                x=alt.X("TASK_SUBNAME:N", title=None,
                        axis=alt.Axis(
                            labelAngle=-90,
                            labelExpr="split(datum.value, ' ')"
                            )
                        ),
                y=alt.Y(f"{agg_fx}({metric}):Q", title=None),
                xOffset=alt.XOffset("WAREHOUSE_SIZE:N", sort=["Medium", "Large", "X-Large"]),
                color=alt.condition(
                    cls._selection,
                    alt.Color(
                        "WAREHOUSE_SIZE:N",
                        title="Warehouse size",
                        scale=alt.Scale(
                            domain=["Medium", "Large", "X-Large"],
                            range=["#872D60", "#E69B43", "#DB5346"]
                        )
                    ),
                    alt.value("lightgray")  # Gray color for unselected bars
                ),
                tooltip=[
                    alt.Tooltip("TASK_SUBNAME:N", title="Task"),
                    alt.Tooltip("WAREHOUSE_SIZE:N", title="Warehouse size"),
                    alt.Tooltip("mean(TOTAL_ELAPSED_TIME_S):Q", title="Mean Total elapsed time (s)", format=".4f"),
                    alt.Tooltip("mean(COMPILATION_TIME_S):Q", title="Mean Compilation time (s)", format=".4f"),
                    alt.Tooltip("mean(QUEUED_OVERLOAD_TIME_S):Q", title="Mean Queued overload time (s)", format=".4f"),
                    alt.Tooltip("mean(EXECUTION_TIME_S):Q", title="Mean Execution time (s)", format=".4f"),
                    alt.Tooltip("mean(TOTAL_CREDITS):Q", title="Mean € Total Credits", format=".0f"),
                    alt.Tooltip("mean(COST_STANDARD):Q", title="Mean € Cost Standard Ed.", format=".4f"),
                    alt.Tooltip("mean(COST_ENTERPRISE):Q", title="Mean € Cost Enterprise Ed.", format=".4f"),
                    alt.Tooltip("mean(COST_BUSINESS_CRITICAL):Q", title="Mean € Cost Business Crit. Ed.", format=".4f")
                ]
            )
            .add_params(cls._selection)
        )

        # Create a separate chart for text labels
        text_label = alt.Chart(df_fltrd).mark_text(
            align='center',
            baseline='middle',
            dy=-10,
            fontSize=10,
            color="white"
        ).encode(
            x=alt.X("TASK_SUBNAME:N"),
            y=alt.Y(f"{agg_fx}({metric}):Q"),
            xOffset=alt.XOffset("WAREHOUSE_SIZE:N", sort=["Medium", "Large", "X-Large"]),
            text=alt.Text(f"{agg_fx}({metric}):Q", format=".1f")
        )

        final_chart = alt.layer(chart, text_label).resolve_scale(y='shared')

        # Display the chart
        cls._chart_state = st.altair_chart(final_chart, use_container_width=True)

        if st.button("refresh", on_click=cls.display_as_table()):
            pass

    @classmethod
    def display_as_table(cls) -> None:
        """ View the dataset filtered as a table """
        st.write(cls._selection)

        # apply the filters (sidebar + chart selection)
        df_fltrd = cls.apply_filters(True, True)

        # display the dag stats rows details
        st.dataframe(df_fltrd)

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H DAG Task performance insights</h1>",
            unsafe_allow_html=True)

        # display the sidebar
        cls.display_sidebar()

        # display the dag runs dataset as chart
        cls.display_as_chart()

        # display the dag runs dataset as table
        cls.display_as_table()


if __name__ == "__main__":
    # set home page properties
    my_page = TasksMonitorPage()

    # display the page
    my_page.display()
