"""..."""
from pandas import DataFrame as pdDataFrame
import altair as alt
import streamlit as st
from modules.st_utils.page_template import PageTemplate


class PerformancePage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()

    @classmethod
    @st.cache_data(ttl="1d")
    def get_data_from_query_history(cls,
                                    timeframe_option: str = "day",
                                    timeframe_depth_option: int = 30
                                    ) -> pdDataFrame:
        """ get copy into executions from query history"""
        df = cls._sf_session.sql(f"""
            select
                --query attributes,
                query_id,
                upper(
                    regexp_substr(
                        query_text,
                        'lineitem|nation|region|orders|supplier|part|partsupp|customer',
                        1, 1, 'i')) as table_name,
                start_time,
                -- compute information
                warehouse_size,
                -- query duration
                total_elapsed_time/1000 AS total_time_s,
                compilation_time/1000 as compilation_time_s,
                execution_time/1000 as exection_time_s,
                queued_provisioning_time as queued_provisioning_time_s,
                -- rows metrics
                rows_inserted,
                count(query_id)
                    over (partition by table_name
                        order by warehouse_size, start_time desc)
                                as run_number,
            from snowflake.account_usage.query_history Q
            where
                to_date(Q.start_time) > dateadd(
                    {timeframe_option},
                    -{timeframe_depth_option},
                    to_date(current_timestamp()))
            and execution_status = 'SUCCESS'
            and warehouse_name='LOAD'
            and query_type='COPY'
            and database_name='MEETUP_GDDP'
            qualify run_number <= 10
            """)
        return df.to_pandas()

    @classmethod
    def display_data_loading_tab(cls, query_hist: pdDataFrame) -> None:
        """ display data loading tab """
        # displaying data loading tab

        # Input widgets
        qh_fltr = st.columns(3)
        with qh_fltr[0]:
            warehouse_size = st.multiselect(
                "Select warehouse size",
                query_hist["WAREHOUSE_SIZE"].unique(),  # type: ignore
                query_hist["WAREHOUSE_SIZE"].unique()  # type: ignore
                )

        # displaying copy run history
        fltrd_query_hist = query_hist[query_hist["WAREHOUSE_SIZE"]
                                      .isin(warehouse_size)]  # type: ignore

        # define selection on click for table_name
        tbl_click = alt.selection_point(fields=["TABLE_NAME"])

        chart_tbl_rows = (
            alt.Chart().mark_bar()  # type: ignore
            .encode(
                y=alt.Y(
                    "TABLE_NAME:N",
                    sort=alt.EncodingSortField(
                        field="ROWS_INSERTED",
                        op="max",
                        order="descending"
                        ),
                    axis=alt.Axis(title=None),
                    ),
                x=alt.X(
                    'max(ROWS_INSERTED):Q',
                    scale=alt.Scale(type='log'),
                    axis=alt.Axis(format="~s", title=None,
                                  labels=True, grid=False),
                    ),
                color=alt.condition(tbl_click, alt.value("#872D60"),
                                    alt.value("lightgray"))
            )
            .add_params(tbl_click)
            .properties(width=200, height=250,
                        title=alt.Title("Rows inserted", align="center")
                        )
            )

        chart_cpy_total_time = (
            alt.Chart().mark_bar()  # type: ignore
            .encode(
                x=alt.X("TABLE_NAME:N", axis=alt.Axis(title=None)),
                y=alt.Y("TOTAL_TIME_S:Q", title=None),
                color=alt.Color(
                    "WAREHOUSE_SIZE:N",
                    scale=alt.Scale(domain=["Large", "Medium"],
                                    range=["#DC9742", "#CD4E41"]),
                    ),
                xOffset="RUN_NUMBER:N",
                tooltip=["TABLE_NAME", "QUERY_ID", "START_TIME",
                         "TOTAL_TIME_S", "WAREHOUSE_SIZE", "ROWS_INSERTED",
                         "COMPILATION_TIME_S", "EXECTION_TIME_S",
                         "QUEUED_PROVISIONING_TIME_S"
                         ]
            )
            .transform_filter(tbl_click)
            .properties(width=600, height=250,
                        title="Copy into total execution time in seconds")
        )

        chart = alt.hconcat(chart_tbl_rows, chart_cpy_total_time,
                            data=fltrd_query_hist)

        # Display the chart in Streamlit
        st.altair_chart(altair_chart=chart,  # type: ignore
                        use_container_width=True)

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Performances</h1>",
            unsafe_allow_html=True)

        # get query history data and cache it
        query_hist = cls.get_data_from_query_history()

        tab1, tab2 = st.tabs(["Data Loading", "TPC-H queries"])  # type: ignore

        with tab1:
            cls.display_data_loading_tab(query_hist)
        with tab2:
            st.write("todo")  # type: ignore


if __name__ == '__main__':
    # set home page properties
    my_page = PerformancePage()

    # display the page
    my_page.display()
