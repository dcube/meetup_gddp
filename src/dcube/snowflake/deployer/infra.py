"""
Module: dcube.snowflake.deployer
Script: infra.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
import logging
from typing import Any
import yaml  # type: ignore[import-untyped]
from snowflake.snowpark.session import Session
from dcube.snowflake.deployer.tool import replace_env_vars, text_shortener


SQL_GEN_PROP_KEY_UNOUTPUTTED: list[str] = ["tbl_cols"]
SQL_GEN_PROP_KEY_IGNORE_COMMA_AFTER: list[str] = ["tbl_cols", ]
SQL_GEN_PROP_KEY_OUTPUTTED_WITHOUT_EQUAL_SIGN: list[str] = ["cluster by",]


def process_property(prop: Any) -> str:
    """Recursive function to process dictionary and replace environment vars"""
    if isinstance(prop, list):
        return "(" + ", ".join(
            [process_property(item) for item in prop]) + ")"
    elif isinstance(prop, dict):
        props: list[str] = []
        for key, value in prop.items():
            if key.lower() in SQL_GEN_PROP_KEY_OUTPUTTED_WITHOUT_EQUAL_SIGN:
                props.append(f"{key.upper()} {process_property(value)}")
            else:
                props.append(f"{key.upper()} = {process_property(value)}")
        return "(" + ", ".join(props) + ")"
    elif isinstance(prop, bool):
        return "TRUE" if prop else "FALSE"
    elif isinstance(prop, int):
        return str(prop)
    elif isinstance(prop, str):
        return f"'{replace_env_vars(prop)}'"
    else:
        logging.warning("unknown type for property: %s", prop)
        return ""


def generate_sql(resource_type: str, name: str,
                 properties: Any = None) -> str:
    "Function to generate SQL statements for a resource type"
    sql_stmt = f"CREATE {resource_type} IF NOT EXISTS {name} "
    if properties:
        props = []
        for key, value in properties.items():
            # if property is "_" key the value as os
            if key.strip().lower() in SQL_GEN_PROP_KEY_UNOUTPUTTED:
                props.append(value)
            else:
                props.append(f"{key.upper()} = {process_property(value)}")
        sql_stmt += ", ".join(props) + ";"
    logging.debug("sql generated: %s", sql_stmt)
    return sql_stmt


def process_config_files(session: Session, files: list[str]) -> None:
    """ process config yaml files for infra resources"""
    for file in files:
        logging.info("Processing file %s", file)

        # Load the YAML file
        with open(file, "r", encoding="utf-8") as yaml_file:
            _resources = yaml.safe_load(yaml_file)

        # Loop through the resources and generate SQL statements
        for resources_type in _resources["resources"]:
            # get resource type
            resource_type = resources_type["type"]

            # change session role to deploy the resource
            session.use_role(resources_type["deploy_role"])

            # lopp over resources to generate sql and execute it
            for resource in resources_type["objects"]:
                resource_name = resource["name"]
                logging.info("Processing deployment for %s.%s ",
                             resource_type, resource_name)
                sql = generate_sql(resource_type, resource_name,
                                   resource["properties"])

                with session.query_history(True, True) as qh:
                    def print_query_status() -> None:
                        query_id = qh.queries[0].query_id
                        query_status = str(
                            session._conn._conn.get_query_status(query_id)
                            ).split(".")[1]
                        query_text_sub = text_shortener(qh.queries[0].sql_text,
                                                        60, ".")
                        print(f"{query_id}: {query_text_sub} - {query_status}")

                    try:
                        session.sql(sql).collect()
                        print_query_status()
                    except Exception as err:
                        print_query_status()
                        raise err
