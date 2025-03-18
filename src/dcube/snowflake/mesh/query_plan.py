"""..."""
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false
import logging
import os
import time
import concurrent.futures
from typing import Any
from snowflake.snowpark.session import Session
from snowflake.snowpark.async_job import AsyncJob

# Get the logger
log = logging.getLogger(__name__)
# Get or create snowpark session
session: Session = Session.builder.getOrCreate()


class QueryPlanBlock():
    """
    A query plan block is a list of sql statement that can be run
    in parallel or sequential
    """

    def __init__(self) -> None:
        self._block: dict[str, bool | list[str]] = dict()

    def set_block(self, parallel_mode: bool,
                  sql_statements: list[str]) -> None:
        """
        set blobk of sql statment to execute
        """
        self._block = {
            "parallel_mode": parallel_mode,
            "sql_statements": sql_statements
        }

    def get_parallel_mode(self) -> bool:
        """
        check is sql statements in block must run in parallel
        """
        return bool(self._block.get("parallel_mode", False))

    def get_sql_statements(self) -> list[str]:
        """
        get the sql statement contained in a block
        """
        value: Any = self._block.get("sql_statements", [])
        sql_statements: list[str] = (value if isinstance(value, list) and all(
            isinstance(item, str) for item in value) else [])
        return sql_statements


class QueryPlan():
    """
    The class QueryPlan is a list of QueryPlanBlock
    """

    def __init__(self) -> None:
        self._blocks: list[QueryPlanBlock] = list()

    def get_blocks(self) -> list[QueryPlanBlock]:
        """
        Returns the list of QueryPlanBlock
        """
        return self._blocks

    def add_block(self, block: QueryPlanBlock) -> None:
        """
        Add a QueryPlanBlock
        Args:
        - block: a QueryPlanBlock object
        """
        if block:
            self._blocks.append(block)

    def add_blocks(self, blocks: list[QueryPlanBlock]) -> None:
        """
        Add a QueryPlanBlock
        Args:
        - block: a QueryPlanBlock object
        """
        if blocks:
            self._blocks += blocks

    def apply(self) -> None:
        """
        Read the query plan and execute all sql statements
        Args:
        - session: the snowpark session to use
        """

        def job_run_nowait(stmt: str) -> AsyncJob:
            """
            use to run the collect_nowait in parallel
            Args
            - stmt: the query to execute
            Returns
            - AsyncJob
            """
            return session.sql(stmt).collect_nowait()  # type: ignore

        def job_is_done(job: AsyncJob) -> AsyncJob | None:
            """
            use to check if an async job is finished
            Args
            - job: the AsyncJob to check
            Returns
            - AsyncJob if is done otherwise None
            """
            if job.is_done():
                return job
            return None

        # loop over sql statements blocks
        for _, block in enumerate(self._blocks):

            # when parallel_mode is set to true
            async_job_lst: list[AsyncJob] = list()
            if block.get_parallel_mode():
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    # execute the collect_nowait in parallel
                    futures = [
                        executor.submit(job_run_nowait, stmt)
                        for stmt in block.get_sql_statements()
                    ]

                    # list all the async jobs executing in parallel
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            async_job_lst.append(future.result())
                        except Exception as e:
                            log.error("An unattended error occured: %s" % e)
            else:
                # otherwise run the statement serially
                for stmt in block.get_sql_statements():
                    _ = session.sql(stmt).collect()

            # waiting for asynchronous jobs to finsh executing
            if len(async_job_lst) > 0:
                # prepare parallel execution to check job status massively
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    while len(async_job_lst) > 0:
                        log.info(
                            "waiting for %s async jobs",
                            len(async_job_lst))

                        # check the job status of async jobs in parallel
                        futures = [
                            executor.submit(
                                job_is_done,  # type: ignore[arg-type]
                                job) for job in async_job_lst
                        ]

                        # get all the job_is_done results
                        completed_jobs: list[AsyncJob] = list()
                        for future in concurrent.futures.as_completed(futures):
                            result = future.result()
                            if result is not None:
                                completed_jobs.append(result)

                        # Remove completed jobs from the list
                        for job in completed_jobs:
                            async_job_lst.remove(job)

                        # if there are running any jobs still runnning
                        # wait 300 ms before recheck the jobs status
                        if len(async_job_lst) > 0:
                            time.sleep(0.3)

    def save_to_file(self, file_path: str) -> None:
        """
        Save the query plan to text file
        Args:
        - file_path: file and path where to save the query plan as text
        """
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # open the file
        with open(file_path, "w", encoding="utf8") as file:
            # loop over the query blocks
            for qpb in self._blocks:
                file.write("select 'start block';\n")
                # if statements' block have to be run in parallel
                if qpb.get_parallel_mode():
                    file.write("select 'start parallel block';\n")
                # lopp over the statements' block
                for sql in qpb.get_sql_statements():
                    file.write(f"{sql};\n")
                # if statements' block have to be run in parallel
                if qpb.get_parallel_mode():
                    file.write("select 'end parallel block';\n")
                file.write("select 'end block';\n")

            # close the file
            file.close()
