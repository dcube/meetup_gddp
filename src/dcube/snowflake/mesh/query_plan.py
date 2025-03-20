"""..."""
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false
import logging
import os
import time
import concurrent.futures
from snowflake.snowpark.session import Session
from snowflake.snowpark.async_job import AsyncJob

# Get the logger
log = logging.getLogger(__name__)
# Get or create snowpark session
session: Session = Session.builder.getOrCreate()


class QueryPlanBlock:
    """
    A query plan block is a list of SQL statements that can be run
    in parallel or sequentially.
    """

    def __init__(self,
                 name: str,
                 role_to_use: str,
                 parallel_mode: bool,
                 sql_statements: list[str] = []) -> None:
        self._name = name
        self._role_to_use = role_to_use
        self._parallel_mode = parallel_mode
        self._sql_statements = sql_statements

    def add_sql_statement(self, sql: str) -> None:
        """Add a SQL statement to the block."""
        self._sql_statements.append(sql)

    def get_name(self) -> str:
        """Get the name of the block."""
        return self._name

    def get_role_to_use(self) -> str:
        """Get the role to use to execute the SQL statements."""
        return self._role_to_use

    def get_parallel_mode(self) -> bool:
        """Check if SQL statements in the block must run in parallel."""
        return self._parallel_mode

    def get_sql_statements(self) -> list[str]:
        """Get the SQL statements contained in the block."""
        return self._sql_statements


class QueryPlan:
    """
    The class QueryPlan is a list of QueryPlanBlock.
    """

    def __init__(self) -> None:
        self._blocks: list[QueryPlanBlock] = []

    def get_blocks(self) -> list[QueryPlanBlock]:
        """Return the list of QueryPlanBlock."""
        return self._blocks

    def add_block(self, block: QueryPlanBlock) -> None:
        """
        Add a QueryPlanBlock.
        Args:
        - block: a QueryPlanBlock object
        """
        self._blocks.append(block)

    def add_blocks(self, blocks: list[QueryPlanBlock]) -> None:
        """
        Add multiple QueryPlanBlock objects.
        Args:
        - blocks: a list of QueryPlanBlock objects
        """
        self._blocks.extend(blocks)

    def add_block_sql_statement(self, block_name: str, sql: str) -> None:
        """
        Add a SQL statement to a block.
        Args:
        - block_name: the name of the block to add the SQL statement
        - sql: the SQL statement to add
        """
        for block in self._blocks:
            if block.get_name() == block_name:
                block.add_sql_statement(sql)
                break

    def apply(self) -> None:
        """
        Read the query plan and execute all SQL statements.
        """

        def job_run_nowait(stmt: str) -> AsyncJob:
            """
            Use to run the collect_nowait in parallel.
            Args:
            - stmt: the query to execute
            Returns:
            - AsyncJob
            """
            return session.sql(stmt).collect_nowait()  # type: ignore

        def job_is_done(job: AsyncJob) -> AsyncJob | None:
            """
            Use to check if an async job is finished.
            Args:
            - job: the AsyncJob to check
            Returns:
            - AsyncJob if done, otherwise None
            """
            return job if job.is_done() else None

        # Loop over SQL statement blocks
        for block in self._blocks:
            async_job_lst: list[AsyncJob] = []

            if block.get_parallel_mode():
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(job_run_nowait, stmt)
                        for stmt in block.get_sql_statements()
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            async_job_lst.append(future.result())
                        except Exception as e:
                            log.error("An unattended error occurred: %s", e)
            else:
                for stmt in block.get_sql_statements():
                    session.sql(stmt).collect()

            if async_job_lst:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    while async_job_lst:
                        log.info("Waiting for %s async jobs",
                                 len(async_job_lst))
                        futures = [
                            executor.submit(job_is_done, job)  # type: ignore
                            for job in async_job_lst
                        ]
                        completed_jobs = [
                            future.result() for future in
                            concurrent.futures.as_completed(futures)
                            if future.result()
                        ]
                        async_job_lst = [
                            job for job in async_job_lst
                            if job not in completed_jobs
                        ]
                        if async_job_lst:
                            time.sleep(0.3)

    def save_to_file(self, file_path: str) -> None:
        """
        Save the query plan to a text file.
        Args:
        - file_path: file and path where to save the query plan as text
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w", encoding="utf8") as file:
            for qpb in self._blocks:
                file.write(f"select 'start block {qpb.get_name()}';\n")
                if qpb.get_role_to_use():
                    file.write(f"use role {qpb.get_role_to_use()};\n")
                if qpb.get_parallel_mode():
                    file.write("select 'start parallel block';\n")
                for sql in qpb.get_sql_statements():
                    file.write(f"{sql};\n")
                if qpb.get_parallel_mode():
                    file.write("select 'end parallel block';\n")
                file.write(f"select 'end block {qpb.get_name()}';\n")
