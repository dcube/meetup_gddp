import streamlit as st
import altair as alt
import pandas as pd
import os

class ChartDisplay:
    @classmethod
    def apply_filters(cls, sidebar_only, other_filters):
        # Dummy implementation for the example
        return pd.DataFrame({
            'TASK_SUBNAME': ['Task1', 'Task2', 'Task3'],
            'WAREHOUSE_SIZE': ['Medium', 'Large', 'X-Large'],
            'TOTAL_ELAPSED_TIME_S': [100, 200, 300],
            'EXECUTION_TIME_S': [50, 100, 150],
            'QUEUED_OVERLOAD_TIME_S': [10, 20, 30],
            'COMPILATION_TIME_S': [5, 10, 15],
            'TOTAL_CREDITS': [1000, 2000, 3000],
            'COST_STANDARD': [100, 200, 300],
            'COST_ENTERPRISE': [150, 250, 350],
            'COST_BUSINESS_CRITICAL': [200, 300, 400]
        })

    @classmethod
    def display_as_chart(cls) -> None:
        """ Chart the dataset filtered """
        # apply the filters (sidebar only)
        df_fltrd = cls.apply_filters(True, False)

        # Define selection on click for TASK_SUBNAME and WAREHOUSE_SIZE
        cls._selection = alt.selection_multi(fields=["TASK_SUBNAME", "WAREHOUSE_SIZE"], on="click", clear="dblclick")

        # Selector to choose the metric and the aggregate function to chart
        cols_kpi_selector = st.columns([0.3, 0.2, 0.5])
        metric = cols_kpi_selector[0].selectbox(
            label="Choose the metric:",
            options=["TOTAL_ELAPSED_TIME_S", "EXECUTION_TIME_S", "QUEUED_OVERLOAD_TIME_S", "COMPILATION_TIME_S",
                    "TOTAL_CREDITS", "COST_STANDARD", "COST_ENTERPRISE", "COST_BUSINESS_CRITICAL"],
            key="sel_metric"
        )
        agg_fx = cols_kpi_selector[1].selectbox(
            label="Choose the aggregate function:",
            options=["mean", "min", "max", "sum", "std"],
            key="sel_agg_fx"
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
                    alt.Tooltip("TASK_SUBNAME:N", title="Task"),
                    alt.Tooltip("WAREHOUSE_SIZE:N", title="Warehouse size"),
                    alt.Tooltip("mean(TOTAL_ELAPSED_TIME_S):Q", title="Mean Total elapsed time (s)", format=".4f"),
                    alt.Tooltip("mean(COMPILATION_TIME_S):Q", title="Mean Compilation time (s)", format=".4f"),
                    alt.Tooltip("mean(QUEUED_OVERLOAD_TIME_S):Q", title="Mean Queued overload time (s)", format=".4f"),
                    alt.Tooltip("mean(EXECUTION_TIME_S):Q", title="Mean Execution time (s)", format=".4f"),
                    alt.Tooltip("mean(TOTAL_CREDITS):Q", title="Mean Total Credits", format=".0f"),
                    alt.Tooltip("mean(COST_STANDARD):Q", title="Mean Cost Standard Ed.", format=".4f"),
                    alt.Tooltip("mean(COST_ENTERPRISE):Q", title="Mean Cost Enterprise Ed.", format=".4f"),
                    alt.Tooltip("mean(COST_BUSINESS_CRITICAL):Q", title="Mean Cost Business Crit. Ed.", format=".4f")
                ]
            )
            .add_params(selection)
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

        # Display the selected data
        if 'selected_data' not in st.session_state:
            st.session_state.selected_data = pd.DataFrame()

        selected_points = st.session_state.get('selected_points', [])

        if selection:
            selected_points = selection.__dict__.get('_value', [])
            st.session_state.selected_points = selected_points

        selected_data = df_fltrd[df_fltrd.apply(lambda row: (row['TASK_SUBNAME'], row['WAREHOUSE_SIZE']) in selected_points, axis=1)]

        if st.button("refresh"):
            st.write("Selected data:", selection)
            cls._chart_state.items

# Example usage
ChartDisplay.display_as_chart()
