"""
Module: test
Script: load_from_csv.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
import os
import time
import json
import argparse
from typing import Dict
from concurrent.futures import ThreadPoolExecutor
from snowflake.snowpark import Session
from dcube.snowpark.raw_tables import RawTable, WriteMode


def load_table(session: Session, tbl: Dict[str, str | bool]) -> None:
    """ load table """
    try:
        raw_table = RawTable(
            session=session,
            name=str(tbl["table_name"])
            )

        raw_table.load_from_csv(
            location=str(tbl.get("stage_path")),
            file_format=str(tbl.get("file_format")),
            mode=WriteMode(str(tbl.get("mode")).lower()),
            force=bool(tbl.get("force"))
            )

        print(f"load table {str(tbl['table_name'])} succeeded")

    except Exception as err:
        print(f"load table {str(tbl['table_name'])} failed with error {err}")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Load data from on S3 bucket")
    parser.add_argument('--warehouse', type=str, required=True,
                        help="The Snowflake warehouse to use")
    parser.add_argument('--size', type=str, required=True,
                        choices=["XSMALL", "SMALL", "MEDIUM", "LARGE"],
                        help="The size of the warehouse")
    parser.add_argument('--database', type=str, required=True,
                        help="The database where to connect to")
    parser.add_argument('--mode', type=str, required=True,
                        choices=["sequential", "parallel"],
                        help="sequential or parallel")
    parser.add_argument('--tables_definition', type=str, required=True,
                        help="json config tables definition")

    # Parse arguments
    args = parser.parse_args()

    # Create a local Snowpark session
    with Session.builder.getOrCreate() as snowpark_session:
        # use warehouse
        snowpark_session.use_warehouse(args.warehouse)
        snowpark_session.sql(f"""ALTER WAREHOUSE {args.warehouse}
                             SET WAREHOUSE_SIZE = {args.size}
                             """).collect()
        snowpark_session.use_database(args.database)

        # set session query tag
        snowpark_session.query_tag = json.dumps(
                {
                    "program": os.path.basename(__file__),
                    "run": int(time.time())
                }
            )

        # read the tables definition from the json file and load data
        with open(args.tables_definition, mode="r", encoding="utf-8") as file:
            tables_definition = json.load(file)

        if args.mode == "parallel":
            with ThreadPoolExecutor() as executor:
                futures = executor.map(  # type: ignore
                    lambda tbl_def:  # type: ignore
                    load_table(snowpark_session, tbl_def),  # type: ignore
                    tables_definition
                    )
        else:
            for tbl_def in tables_definition:
                load_table(snowpark_session, tbl_def)
