"""
Module: dcube.snowflake.deployer
Script: asset.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
import os
from datetime import datetime
import json
import yaml  # type: ignore[import-untyped]
from snowflake.snowpark.session import Session
from dcube.snowflake.deployer.infra import generate_sql


if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        # define session query tag
        session.query_tag = json.dumps(
            {
                "program": os.path.basename(__file__),
                "run": int(datetime.timestamp(datetime.now()))
            }
        )

        # set data product yaml file to process
        file = "/workspaces/app/src/config/data_products/" \
            "tpch_sf100.yml"

        # open data product definition yml file
        with open(file, "r", encoding="utf-8") as yaml_file:

            asset = yaml.safe_load(yaml_file)

            # get or create the database
            database = asset["data_product"]["database"]
            session.sql(generate_sql("DATABASE", database, None)).collect()
            session.use_database(database)

            # get or create the schema
            schema = asset["data_product"]["schema"]
            schema_name = schema["name"] if "name" in schema else schema
            print(
                # session.sql(
                generate_sql(
                    "SCHEMA",
                    schema_name,
                    schema["properties"] if "properties" in schema else None
                    )
                #    ).collect()
            )

            data_product = f"{database}.{schema_name}"

            # parse data product objects and create them
            for obj in asset["data_product"]["objects"]:
                # generate sql to create database and schema if not exist
                obj_type, _ = next(iter(obj.items()))
                session.sql(
                    generate_sql(
                        obj_type,
                        f"{data_product}.{obj['name']}",
                        obj["properties"] if "properties" in obj else None
                        )
                    ).collect()

        # close the session
        session.close()
