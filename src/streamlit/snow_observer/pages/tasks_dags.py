"""..."""
from sf_utils.tasks import get_dags, get_dags_runs
from st_utils.page_template import PageTemplate
from snowflake.snowpark.dataframe import DataFrame
from datetime import date
import streamlit as st


class TaskPage(PageTemplate):
    """ Task Page"""
    start_date: date
    end_date: date

    @st.cache_data
    def get_dag_runs(_self) -> DataFrame:
        """ get dag runs and set cache """
        return get_dags_runs(_self.sf_session).sort("LAST_RUN", ascending=False).to_pandas()


    def sidebar(self, df_dag_runs: DataFrame) -> None:
        """ sidebar filters"""
        min_date = df_dag_runs["LAST_RUN"].min().date()
        max_date = df_dag_runs["LAST_RUN"].max().date()
        self.start_date, self.end_date = st.sidebar.slider(
            "Date range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
            )

    def view_dags_runs(self, df_dag_runs: DataFrame) -> None:
        """ """
        # filter the dag runs
        df_dag_runs_filtered = df_dag_runs[(df_dag_runs["LAST_RUN"].dt.date >= self.start_date) &
                                  (df_dag_runs["LAST_RUN"].dt.date <= self.end_date)]

        # Combine 'succeeded' and 'failed' into a 'runs' column
        def format_runs(row) -> str:
            succeeded_html = f'<span style="display:inline-block; background-color:green; color:white; border-radius:50%; width:30px; height:30px; text-align:center; line-height:30px; font-weight:bold;">{row["SUCCEEDED"]}</span>'
            failed_html = f'<span style="display:inline-block; background-color:red; color:white; border-radius:50%; width:30px; height:30px; text-align:center; line-height:30px; font-weight:bold;">{row["FAILED"]}</span>'
            return f"{succeeded_html} {failed_html}"

        # Create the 'runs' column using the format_runs function
        df_dag_runs_filtered["RUNS"] = df_dag_runs_filtered.apply(format_runs, axis=1)

        # Drop the original 'succeeded' and 'failed' columns
        df_dag_runs_filtered = df_dag_runs_filtered.drop(["SUCCEEDED", "FAILED"], axis=1)

        # Build an HTML table to display the DataFrame with colored circles
        def render_html_table(df):
            """ """
            html_table = df.to_html(escape=False, index=False)

            # Style headers to center-align
            html_table = html_table.replace('<th>', '<th style="text-align:left;">')

            return html_table

        # Display the styled table
        html_table = render_html_table(df_dag_runs_filtered)
        st.markdown(html_table, unsafe_allow_html=True)

    def render(self) -> None:
        """ rendering this page by override the Template """
        # set page title
        st.title("Tasks Dags")

        # get and cache dag runs data
        df_dag_runs = self.get_dag_runs()

        # sidebar filters
        self.sidebar(df_dag_runs)

        # view the dags runs
        self.view_dags_runs(df_dag_runs)


if __name__ == "__main__":
    # set home page properties
    my_page = TaskPage()

    # render the homepage
    my_page.render()
