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

        # init dags runs
        cls._dags_runs = DagRuns(cls._sf_session, "AWS_EU_PARIS")

        cls._dataset = cls._dags_runs.get_dag_run_stats()

        # dropdown definitions for sidebar
        cls._sidebar_filters = [
            {"col": "WORKLOAD_TYPE", "key": f"{os.path.basename(__file__)}._fltr_workload_type", "label": "Select workload"},
            {"col": "RUN_MODE", "key": f"{os.path.basename(__file__)}._fltr_run_mode", "label": "Select run mode"},
            {"col": "DATA_FORMAT", "key": f"{os.path.basename(__file__)}._fltr_dta_fmt", "label": "Select data format"},
            {"col": "WAREHOUSE_SIZE", "key": f"{os.path.basename(__file__)}._fltr_wh_size", "label": "Select warehouse size"}
        ]

        # default session state for chart metric and agg fx
        if not st.session_state.get(f"{os.path.basename(__file__)}._sel_metric"):
            st.session_state[f"{os.path.basename(__file__)}._sel_metric"] = "DAG_DURATION_S"
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
                    df_filtr = df_filtr[df_filtr[cls._sidebar_filters[idx]["col"]].isin(selected_values)]

        if chart_selection:
            # apply chart selection filters
            df_chart_sel = PdDataFrame(PdDataFrame(cls._chart_state.selection)["param_1"].to_list())

            if len(df_chart_sel) > 0:
                df_filtr = df_filtr.merge(
                    df_chart_sel,
                    on=["DAG_FORMAT", "WAREHOUSE_SIZE"],
                    how="inner")

        return df_filtr

    @classmethod
    def display_as_chart(cls) -> None:
        """ Chart the dataset filtered """
        # apply the filters (sidebar only)
        df_fltrd = cls.apply_filters(True, False)

        # Define selection on click for DAG_NAME, WAREHOUSE_SIZE, and DATA_FORMAT
        selection = alt.selection_point(fields=["DAG_FORMAT", "WAREHOUSE_SIZE"], on="click")

        # Selector to choose the metric and the aggregate function to chart
        cols_kpi_selector = st.columns([0.2, 0.2, 0.6])
        metric = cols_kpi_selector[0].selectbox(
            label="Choose the metric:",
            options=["DAG_DURATION_S", "DAG_CREDITS", "COST_STANDARD", "COST_ENTERPRISE", "COST_BUSINESS_CRITICAL"],
            key=f"{os.path.basename(__file__)}._sel_metric"
            )
        agg_fx = cols_kpi_selector[1].selectbox(
            label="Choose the metric:",
            options=["mean", "min", "max", "sum", "std"],
            key=f"{os.path.basename(__file__)}._sel_agg_fx"
            )

        # chart dag runs stats
        chart_dags = (
            alt.Chart(df_fltrd)
            .mark_bar()  # type: ignore
            .encode(
                x=alt.X("DAG_FORMAT:N", title=None,
                        axis=alt.Axis(
                            labelAngle=0,
                            labelExpr="split(datum.value, ' ')"
                            )
                        ),
                y=alt.Y(f"{agg_fx}({metric}):Q", title=None),
                xOffset=alt.XOffset("WAREHOUSE_SIZE:N", sort=["Medium", "Large", "X-Large"]),
                color=alt.condition(
                    selection,
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
                        alt.Tooltip("DAG_NAME:N", title="Task"),
                        alt.Tooltip("DATA_FORMAT:N", title="Task"),
                        alt.Tooltip("WAREHOUSE_SIZE:N", title="Warehouse size"),
                        alt.Tooltip(f"{agg_fx}(DAG_DURATION_S):Q", title=f"{agg_fx} Total elapsed time (s)", format=".4f"),
                        alt.Tooltip(f"{agg_fx}(ROWS_PRODUCED):Q", title=f"{agg_fx} Rows Produced", format=".0f"),
                        alt.Tooltip(f"{agg_fx}(COST_STANDARD):Q", title=f"{agg_fx} Cost Standard Ed.", format=".4f"),
                        alt.Tooltip(f"{agg_fx}(COST_ENTERPRISE):Q", title=f"{agg_fx} Cost Enterprise Ed.", format=".4f"),
                        alt.Tooltip(f"{agg_fx}(COST_BUSINESS_CRITICAL):Q", title=f"{agg_fx} Cost Business Crit. Ed.", format=".4f")
                    ]
            )
            .add_params(selection)
        )

        # display the chart
        cls._chart_state = st.altair_chart(
            chart_dags,
            use_container_width=True,
            on_select="rerun")

    @classmethod
    def display_as_metrics(cls) -> None:
        """ View the dataset filtered as metrics """
        # display the metric
        df_fltrd = cls.apply_filters(True, True)

        def get_kpi(df: PdDataFrame, col: str, type: str, fmt: str) -> str :
            """ get kpi value and format it for metrics"""
            if type == "Tot":
                kpi = df[col].sum()
            elif type == "Mean":
                kpi = df[col].mean()

            kpi = fmt.format(kpi) if kpi != 0 and kpi != float("nan")  else "-"
            return kpi

        # Display 2 lines of metrics one the main kpi as Sum and Mean
        for type in ["Tot", "Mean"]:
            kpi = st.columns([20, 20, 15, 15, 10, 10, 10])
            kpi[0].metric(f"{type}. rows inserted",
                            get_kpi(df_fltrd[df_fltrd["WORKLOAD_TYPE"] == 'LOAD'],
                                    "ROWS_PRODUCED", type, "{:,.0f}"))
            kpi[1].metric(f"{type}. rows returned",
                            get_kpi(df_fltrd[df_fltrd["WORKLOAD_TYPE"] == 'NLITX'],
                                    "ROWS_PRODUCED", type, "{:,.0f}"))
            kpi[2].metric(f"{type}. Paritions scanned", get_kpi(df_fltrd, "PARTITIONS_SCANNED", type, "{:,.0f}"))
            kpi[3].metric(f"{type}. Partitions", get_kpi(df_fltrd, "PARTITIONS_TOTAL", type, "{:,.0f}"))
            kpi[4].metric(f"{type}. € Std. Ed.", get_kpi(df_fltrd, "COST_STANDARD", type, "{:,.2f}"))
            kpi[5].metric(f"{type}. € Entr. Ed.", get_kpi(df_fltrd, "COST_ENTERPRISE", type, "{:,.2f}"))
            kpi[6].metric(f"{type}. € BizCrit. Ed.", get_kpi(df_fltrd, "COST_BUSINESS_CRITICAL", type, "{:,.2f}"))

    @classmethod
    def display_as_table(cls) -> None:
        """ View the dataset filtered as a table """
        # apply the filters (sidebar + chart selection)
        df_fltrd = cls.apply_filters(True, True)

        # display the dag stats rows details
        st.dataframe(df_fltrd)

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H DAG performance insights</h1>",
            unsafe_allow_html=True)

        # display the sidebar
        cls.display_sidebar()

        # display the dag runs dataset as chart
        cls.display_as_chart()

        # display the dag runs dataset as metrics
        cls.display_as_metrics()

        # display the dag runs dataset as table
        cls.display_as_table()


if __name__ == "__main__":
    # set home page properties
    my_page = DagsMonitorPage()

    # display the page
    my_page.display()
