"""
Module: test
Script: load_from_csv.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor
from snowflake.snowpark import Session
from dcube.snowpark.table_functions import load_from_csv


TABLES_TO_LOAD = ["CUSTOMER", "LINEITEM", "NATION", "ORDERS",
                  "PART", "PARTSUPP", "REGION", "SUPPLIER", ]
DATA_PRODUCT = "MEETUP_GDDP.TPCH_SF100"
STAGE_PATH = "MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv"
FILE_FORMAT = "MEETUP_GDDP.UTILS.CSV_FMT1"


def load_from_csv_task(session: Session, schema_name: str, stage_path: str,
                       table_name: str) -> None:
    """load table from csv"""
    load_from_csv(
        session=session,
        schema_name=schema_name,
        table_name=table_name,
        location=f"@{stage_path}/{table_name.lower()}/",
        file_format=FILE_FORMAT,
        write_mode="truncate"
    )


if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as snowpark_session:
        # use warehouse
        snowpark_session.use_warehouse("LOAD")

        # set session query tag
        snowpark_session.query_tag = json.dumps(
                {
                    "program": os.path.basename(__file__),
                    "run": int(time.time())
                }
            )

        # Use ThreadPoolExecutor to parallelize table creation
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(load_from_csv_task, snowpark_session,
                                DATA_PRODUCT, STAGE_PATH, table)
                for table in TABLES_TO_LOAD
            ]

        # Optional: Wait for all tasks to complete and handle exceptions
        for future in futures:
            try:
                future.result()  # Check for exceptions
            except Exception as e:
                print(f"Error loading table: {e}")
