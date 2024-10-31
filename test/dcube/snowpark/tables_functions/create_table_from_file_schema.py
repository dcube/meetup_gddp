"""
Module: test
Script: create_table_from_file_schema.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor
from snowflake.snowpark import Session
from dcube.snowpark.table_functions import create_table_from_file_schema


TABLES_TO_CREATE = ["CUSTOMER", "LINEITEM", "NATION", "ORDERS",
                    "PART", "PARTSUPP", "REGION", "SUPPLIER", ]
DATA_PRODUCT = "MEETUP_GDDP.TPCH_SF100"
STAGE_PATH = "MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv"
FILE_FORMAT = "MEETUP_GDDP.UTILS.CSV_FMT1"


def create_table_task(session: Session, schema_name: str, stage_path: str,
                      table_name: str, file_format: str) -> None:
    """create or replace table using schema inference from files on stage"""
    create_table_from_file_schema(
        session=session,
        schema_name=schema_name,
        table_name=table_name,
        location=f"@{stage_path}/{table_name.lower()}/",
        file_format=file_format,
        max_file_count=5,
        max_records_per_file=300000,
        overwrite=True
    )


if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as snowpark_session:
        # set session tag
        snowpark_session.query_tag = json.dumps(
                {
                    "program": os.path.basename(__file__),
                    "run": int(time.time())
                }
            )

        # use warehouse
        snowpark_session.use_warehouse("MANAGE")

        # Use ThreadPoolExecutor to parallelize table creation
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(create_table_task, snowpark_session,
                                DATA_PRODUCT, STAGE_PATH,
                                table, FILE_FORMAT)
                for table in TABLES_TO_CREATE
            ]

        # Optional: Wait for all tasks to complete and handle exceptions
        for future in futures:
            try:
                future.result()  # Check for exceptions
            except Exception as e:
                print(f"Error creating table: {e}")
