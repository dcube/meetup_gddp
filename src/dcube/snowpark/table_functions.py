"""
Module: dcube.snowpark
Script: table_functions.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
from typing import Dict
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, listagg, concat, col


def table_exists(session: Session, schema_name: str, table_name: str) -> bool:
    """ check if table exists """
    df = session.table(f"{schema_name}.{table_name}")
    try:
        # if dataframe has schema => table exists
        _ = df.schema
        return True
    except SnowparkSQLException:
        # object doesn't exist
        return False


def create_table_from_file_schema(session: Session, schema_name: str,
                                  table_name: str, location: str,
                                  file_format: str, ignore_case: bool = True,
                                  max_file_count: int = 5,
                                  max_records_per_file: int = 300000,
                                  schema_evol: bool = False,
                                  overwrite: bool = False,
                                  cluster_by: list[str] | None = None,
                                  iceberg_config: Dict[str, str] | None = None
                                  ) -> None:
    """create table, if not exists, with schema inference and shema drifting"""
    session.use_schema(schema_name)

    # check if table doesn't exists or if we want to replace table if it exists
    if not table_exists(session, schema_name, table_name) or overwrite:
        # Read stage files to infer schema
        df = session.table_function(  # type: ignore[reportUnknownMemberType]
            func_name="infer_schema", location=lit(location),
            file_format=lit(file_format), max_file_count=lit(max_file_count),
            max_records_per_file=lit(max_records_per_file),
            ignore_case=lit(ignore_case)) \
              .select(
                  listagg(
                      concat(
                          col("COLUMN_NAME"),
                          lit(" "),
                          col("TYPE")
                          ), ", ").within_group("order_id"))

        # build iceberg properties if needed
        iceberg: str = ""
        if iceberg_config:
            iceberg = f"""catalog = 'SNOWFLAKE'
              external_volume = '{iceberg_config['EXTERNAL_VOLUME']}'
              base_location = '{iceberg_config['BASE_LOCATION']}'
              """

        # drop the table if it already exists
        if overwrite:
            session.sql(f"drop table if exists {table_name}").collect()

        # create the table
        session.sql(f"""
          create {"iceberg" if iceberg_config else ""}
          table {table_name}
            ( {df.collect()[0][0]})
          with
            { f"enable_schema_evolution = {schema_evol}"
             if not iceberg_config else ""}
            { iceberg }
          """).collect()

        # define clustering keys if needed
        if cluster_by:
            session.sql(f"""alter {"iceberg" if iceberg_config else ""}
                        table {table_name}
                        cluster by ({', '.join(cluster_by)})""").collect()


def load_from_csv(session: Session, schema_name: str, table_name: str,
                  location: str, file_format: str, write_mode: str = "append"
                  ) -> None:
    """Read csv and load into table"""
    session.use_schema(schema_name)

    # get the table data schema
    data_schema = session.table(table_name).schema

    # read csv from stage
    df = session.read.option("format_name", file_format) \
        .schema(data_schema).csv(location)

    # write to table
    df.write.mode(write_mode).save_as_table(table_name)


if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as snowpark_session:
        # use warehouse
        snowpark_session.use_warehouse("MANAGE")

        # create table from stage files with schema inference
        create_table_from_file_schema(
            session=snowpark_session,
            schema_name="MEETUP_GDDP.TPCH_SF100",
            table_name="TEST",
            location="@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/lineitem",
            file_format="MEETUP_GDDP.UTILS.CSV_FMT1",
            max_file_count=5,
            max_records_per_file=300000,
            overwrite=True,
            cluster_by=["L_SHIPDATE"],
            iceberg_config={
                "EXTERNAL_VOLUME": "MEETUP_GDDP_S3_LAKEHOUSE",
                "BASE_LOCATION": "MEETUP_GDDP/TPCH_SF100_ICEBERG/TEST"
              }
        )
