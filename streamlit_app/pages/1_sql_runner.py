# type: ignore
# pylint: disable=import-error
"""..."""
import os
import re
import json
import time
from datetime import datetime
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pandas import DataFrame as pdDataFrame
import streamlit as st
from modules.st_utils.page_template import PageTemplate


class ExecuteSQLPage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        # Call the PageTemplace class constructor
        super().__init__()
        cls._sf_session.use_database("MEETUP_GDDP")

        # retrieve list of virtual warehouses
        cls.set_virtual_warehouses()

        # retrieve list of database schema
        cls.set_database_schemas()

        # init session state and default values
        if f"{os.path.basename(__file__)}._force_new_session" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._force_new_session"] = True
        if f"{os.path.basename(__file__)}._session_cold_start" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._session_cold_start"] = True
        if f"{os.path.basename(__file__)}._cache_query" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._cache_query"] = True
        if f"{os.path.basename(__file__)}._run_mode" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._run_mode"] = "PARALLEL"
        if f"{os.path.basename(__file__)}._input_mode" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._input_mode"] = "UPLOAD"
        if f"{os.path.basename(__file__)}._warehouse_name" not in st.session_state:
            st.session_state[f"{os.path.basename(__file__)}._warehouse_name"] = cls._warehouses["name"][0]
        if f"{os.path.basename(__file__)}._warehouse_size" not in st.session_state:
            # get the size of the selected warehouse
            st.session_state[f"{os.path.basename(__file__)}._warehouse_size"] = str(
                str(cls._warehouses.loc[cls._warehouses["name"]
                                        == st.session_state[f"{os.path.basename(__file__)}._warehouse_name"]
                                        ]["size"].values[0])
            )

    @classmethod
    def set_virtual_warehouses(cls) -> None:
        """list the virtual warehouse"""
        # get warehouses descriptions without need of compute
        _ = cls._warehouses = cls._sf_session.sql("SHOW WAREHOUSES").collect()

        df_warehouses = cls._sf_session.sql("SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID()))").to_pandas()

        # set the virtual warehouses list
        cls._warehouses = df_warehouses

    @classmethod
    def resize_virtual_warehouse(cls) -> None:
        """ resize the selected warehouse if needed """
        # get the current size of the selected warehouse
        wh_name = st.session_state[f"{os.path.basename(__file__)}._warehouse_name"]

        cur_wh_size: str = str(
            cls._warehouses
            .loc[cls._warehouses["name"] == wh_name]["size"].values[0]
            )

        tgt_wh_size = st.session_state[f"{os.path.basename(__file__)}._warehouse_size"]

        if cur_wh_size != tgt_wh_size:
            # change the warehouse size
            cls._sf_session.sql(
                f"""ALTER WAREHOUSE {wh_name}
                SET WAREHOUSE_SIZE = '{tgt_wh_size}'
                """
                ).collect()


    @classmethod
    def set_database_schemas(cls) -> None:
        """list the database schema"""
        # get warehouses descriptions without need of compute
        _ = cls._sf_session.sql("SHOW SCHEMAS LIKE 'TPCH_SF100%'").collect()

        df_schemas = cls._sf_session.sql("SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID()))").to_pandas()

        # add calc column with wh name and size
        df_schemas["database_schema"] = df_schemas["database_name"] + "." + df_schemas["name"]

        # set the virtual warehouses list
        cls._db_schemas = df_schemas

    @staticmethod
    def string_query_parser(sql_text: str) -> list[str]:
        """parse a string which contains queries separated by ;
        and return a list of query string"""
        queries: list[str] = []

        if sql_text:
            comment_pattern = re.compile(r"""
                    (?:'[^'\\]*(?:\\.[^'\\]*)*') |  # Single quoted strings
                    (?:"[^"\\]*(?:\\.[^"\\]*)*") |  # Double quoted strings
                    (?:--.*?$) |  # Single-line comments
                    (?:\/\/.*?$) |  # Single-line comments with //
                    (?:\/\*.*?\*\/)  # Multi-line comments
                """, re.VERBOSE | re.DOTALL | re.MULTILINE)

            # Function to preserve quoted strings
            def preserve_quoted_strings(match):
                if match.group(0).startswith(('--', '/*', '//')):
                    return ''  # Remove comments
                return match.group(0)  # Preserve quoted strings

            # Remove comments from the SQL text
            clean_sql_text = re.sub(comment_pattern, preserve_quoted_strings, sql_text)

            # Split the cleaned SQL text into individual statements
            statements = re.split(r';\s*', clean_sql_text)

            # Remove any empty statements
            queries = [stmt for stmt in statements if stmt]

        return queries

    @staticmethod
    def string_query_shortener(sql_text: str, max_length: int,
                               fill_char: str) -> str:
        """ print query history from the session """
        shortened = re.sub(r"\s+",
                           " ",
                           re.sub(r"[\t\n\r]", "", sql_text)
                           )[:max_length].ljust(max_length, fill_char)
        return shortened

    @classmethod
    def __run_batch_query(cls, sql_query: str) -> Dict[str, str]:
        """ run the sql query passed in parameter and return true
        if the query succeeded else false"""
        job_result: Dict[str, str] = {"query": sql_query}

        # run the query async
        start_time = time.time()
        job = cls._sf_session.sql(sql_query).collect_nowait()

        # wait till the query is done
        while True:
            if job.is_done():
                end_time = time.time()
                break

        # get query execution information from the query_history
        df_query = cls._sf_session.sql(
            f"""
            select
                query_text, query_id, execution_status as status,
                start_time, end_time, total_elapsed_time/1000 as duration
            from
                table(information_schema.query_history_by_session())
            where query_id = '{job.query_id}'
            """
            ).to_pandas()

        # add the query id to the job_result dict
        job_result["query_id"] = job.query_id

        # add the query id to the job_result dict
        # job_result["status"] = str(cls._sf_session._conn._conn.get_query_status(job.query_id)).split(".")[1]
        job_result["status"] = df_query["STATUS"][0]

        # add the query id to the job_result dict
        # job_result["start_time"] = datetime.fromtimestamp(start_time).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
        job_result["start_time"] = df_query["START_TIME"][0]

        # add the query id to the job_result dict
        # job_result["end_time"] = datetime.fromtimestamp(end_time).strftime("%d/%m/%Y %H:%M:%S.%f")[:-3]
        job_result["end_time"] = df_query["END_TIME"][0]

        # job duration
        # job_result["duration"] = end_time - start_time
        job_result["duration"] = df_query["DURATION"][0]

        return job_result

    @classmethod
    def run_batch_queries(cls, sql_queries: str) -> None:
        """parse and run queries"""
        if sql_queries:
            with st.spinner("Executing all queries..."):
                # check if we have to re-create a session
                if cls._force_new_session:
                    cls._sf_session.close()
                    cls.set_session()

                # check if query result set must be disabled for the session
                if cls._cache_query:
                    cls._sf_session.sql(
                        "ALTER SESSION SET USE_CACHED_RESULT = FALSE"
                        ).collect()

                # select the warehouse
                sel_warehouse: str = st.session_state[f"{os.path.basename(__file__)}._warehouse_name"]

                # check if warehouse must be suspended for cold start
                if (cls._session_cold_start and
                    cls._warehouses.loc[
                        cls._warehouses["name"]
                        == sel_warehouse, "state"].values[0]
                        != "SUSPENDED"):

                    # suspend the warehouse
                    cls._sf_session.sql(
                        f"ALTER WAREHOUSE {sel_warehouse} SUSPEND"
                        ).collect()

                # set the default schema
                if len(cls._database_schema) > 0:
                    cls._sf_session.use_schema(cls._database_schema)

                # execute queries
                cls._sf_session.query_tag = json.dumps(
                        {
                            "domain": "MEETUP_GDDP",
                            "kind": "streamlit",
                            "module": "sql runner",
                            "run_time": int(time.time()),
                            "run_mode": cls._run_mode,
                            "cache_query": cls._cache_query
                        }
                    )

                cls._sf_session.use_warehouse(sel_warehouse)

                def show_results(result: Dict[str, str]) -> None:
                    res = []
                    res.append(result)
                    try:
                        cls._tbl_result.add_rows(pdDataFrame.from_dict(res))
                    except Exception:
                        cls._tbl_result = st.dataframe(pdDataFrame.from_dict(res))

                # check run mode parallel or sequential
                if str(cls._run_mode) == "PARALLEL":
                    with ThreadPoolExecutor() as executor:
                        futures = {
                            executor.submit(cls.__run_batch_query, query):
                            query for
                            query in cls.string_query_parser(sql_queries)
                            }

                        for future in as_completed(futures):
                            query = futures[future]
                            show_results(future.result())
                else:
                    # exec queries sequentially
                    for query in cls.string_query_parser(sql_queries):
                        show_results(cls.__run_batch_query(query))

    @classmethod
    def display_sidebar(cls) -> None:
        """display the page sidebar """
        # check box to force to use a new snowpark session
        # (default value is True)
        cls._force_new_session = st.sidebar.checkbox(
            "Force new session",
            key=f"{os.path.basename(__file__)}._force_new_session"
            )

        # check box to force to execute queries
        # with warehouse suspended (default value is True)
        cls._session_cold_start = st.sidebar.checkbox(
            "Session cold start",
            key=f"{os.path.basename(__file__)}._session_cold_start"
            )

        # check box to disable the cache query result set
        # (default value is True)
        cls._cache_query = st.sidebar.checkbox(
            "Disable cache query",
            key=f"{os.path.basename(__file__)}._cache_query")

        # radio button to switch between parallel and sequential runs
        cls._run_mode = st.sidebar.radio(
            "Select a run mode",
            options=["PARALLEL", "SEQUENTIAL"],
            key=f"{os.path.basename(__file__)}._run_mode"
            )

        # radio button to switch to upload files or text area for your
        # sql queries entry
        cls._input_mode = st.sidebar.radio(
            "Upload or input SQL queries",
            options=["UPLOAD", "INPUT"],
            key=f"{os.path.basename(__file__)}._input_mode"
            )

        # dropdown to select the default database schema
        cls._database_schema = st.sidebar.selectbox(
            "Select the default database schema",
            options=cls._db_schemas["database_schema"],
            key=f"{os.path.basename(__file__)}._database_schema"
            )

        # dropdown to select the warehouse to use to execute the queries
        cls._warehouse_name = st.sidebar.selectbox(
            "Select a warehouse",
            options=cls._warehouses["name"],
            key=f"{os.path.basename(__file__)}._warehouse_name"
            )

        # dropdown to resize the warehouse
        # (default value is the size of the selected warehouse)
        cls._warehouse_size = st.sidebar.selectbox(
            "Choose warehouse size",
            options=["X-Small", "Small", "Medium", "Large"],
            on_change=cls.resize_virtual_warehouse(),
            key=f"{os.path.basename(__file__)}._warehouse_size"
            )

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>Execute SQL queries</h1>",
            unsafe_allow_html=True)

        cls.display_sidebar()

        # depending on input_mode show text_are of file_uploader component
        queries: str = ""
        if cls._input_mode == "INPUT":
            # Text area to input queries
            queries = st.text_area(
                "Enter queries separated by ; or upload *.sql",
                height=200
                )

        elif cls._input_mode == "UPLOAD":
            uploaded_files = st.file_uploader(
                "Choose SQL files", accept_multiple_files=True
                )
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    queries += uploaded_file.read().decode("utf-8")
                if st.button("View code"):
                    with st.expander(label="queries", expanded=True):
                        st.code(body=queries, language="sql")

        # show button [Execute queries] if queries is not empty
        if len(queries) > 0:
            if st.button("Execute Queries"):
                # run queries when clicking
                cls.run_batch_queries(queries)


if __name__ == '__main__':
    # set home page properties
    my_page = ExecuteSQLPage()

    # display the page
    my_page.display()
