"""
Module: dcube.snowpark
Script: raw_tables.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
from typing import Any, Dict, List
from enum import Enum
import re
from snowflake.snowpark import Session
from snowflake.snowpark.row import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, listagg, concat, col


class WriteMode(Enum):
    """Enum values to define the write mode"""
    APPEND = "append"
    TRUNCATE = "truncate"


class RawTable:
    """ Class for raw tables """
    def __init__(self, session: Session, name: str) -> None:
        """ class constructor"""
        self._session = session
        self._name = name

    def exists(self) -> bool:
        """ check if table exists use fully qualified name for table_name """
        df = self._session.table(self._name)
        try:
            # if dataframe has schema => table exists
            _ = df.schema
            return True
        except SnowparkSQLException:
            # object doesn't exist
            return False

    def __replace_number_precision(self, sql: str) -> str:
        """ ignore number preceision """
        # Regular expression to match NUMBER(p, s) and replace p with 38
        pattern = re.compile(r"NUMBER\(\d+,")
        # Replace the matched pattern with NUMBER(38,
        modified_sql = pattern.sub("NUMBER(38,", sql)
        return modified_sql

    def create_by_file_inference(self,
                                 location: str,
                                 file_format: str,
                                 **inference_options: Any
                                 ) -> None:
        """Create table, if not exists, with schema inference
        and shema drifting base on a files on stage location
        Use fully qualified name for table_name and file_format
        """
        max_file_count = inference_options.get("max_file_count", 5)
        max_records_per_file = inference_options.get("max_records_per_file",
                                                     30000)
        ignore_case = inference_options.get("ignore_case", True)
        schema_evol = inference_options.get("schema_evol", False)
        overwrite = inference_options.get("overwrite", False)
        cluster_by = inference_options.get("cluster_by", None)
        iceberg_config = inference_options.get("iceberg_config", None)

        # check if table doesn't exists
        if not self.exists():
            # Read stage files to infer schema
            df = self._session.table_function(
                func_name="infer_schema",
                location=lit(location),
                file_format=lit(file_format),
                max_file_count=lit(max_file_count),
                max_records_per_file=lit(max_records_per_file),
                ignore_case=lit(ignore_case)) \
                .select(
                    listagg(
                        concat(
                            col("COLUMN_NAME"),
                            lit(" "),
                            col("TYPE")
                            ), ", ").within_group("ORDER_ID")).collect()

            # build iceberg properties if needed
            iceberg: str = ""
            if iceberg_config:
                iceberg = f"""CATALOG = 'SNOWFLAKE'
                EXTERNAL_VOLUME = '{iceberg_config['EXTERNAL_VOLUME']}'
                BASE_LOCATION = '{iceberg_config['BASE_LOCATION']}'
                """

            # drop the table if it already exists
            if overwrite:
                self._session.sql(f"DROP TABLE IF EXISTS {self._name}")\
                    .collect()

            # create the table
            try:
                sql_stmt = f"""
                    CREATE {"ICEBERG" if iceberg_config else ""}
                    TABLE {self._name}
                        ( {self.__replace_number_precision(str(df[0][0]))})
                    WITH
                        { f"ENABLE_SCHEMA_EVOLUTION = {schema_evol}"
                        if not iceberg_config else ""}
                        { iceberg }
                    """
                self._session.sql(sql_stmt).collect()
            except SnowparkSQLException as err:
                raise err

            # define clustering keys if needed
            if cluster_by:
                self._session.sql(f"""
                                  alter {"iceberg" if iceberg_config else ""}
                                  table {self._name}
                                  cluster by ({', '.join(cluster_by)})
                                  """
                                  ).collect()

    def load_from_csv(self,
                      location: str,
                      file_format: str,
                      mode: WriteMode = WriteMode.APPEND,
                      match_by_column_name: str = "case_insensitive",
                      force: bool = False,
                      **inference_options: Any
                      ) -> List[Row]:
        """Read csv and load into table use fully qualified name
        for table_name and file_format"""

        # test if table exists and create it if needed
        if not self.exists():
            self.create_by_file_inference(
                location=location,
                file_format=file_format,
                inference_options=inference_options
                )

        # get the table data schema
        data_schema = self._session.table(self._name).schema

        # read csv from stage
        df = self._session.read.option("format_name", file_format) \
            .schema(data_schema).csv(location)

        if mode == WriteMode.TRUNCATE:
            self._session.sql(f"TRUNCATE TABLE {self._name}")

        # write to table
        rows = df.copy_into_table(table_name=self._name,
                                  match_by_column_name=match_by_column_name,
                                  force=force)

        return rows


def load_from_csv_old(session: Session, tbl: Dict[str, str | bool]) -> None:
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


def load_from_csv(session: Session,
                  tbl_config: Dict[str, str | bool]) -> List[Row]:
    """ load table """
    try:
        raw_table = RawTable(
            session=session,
            name=str(tbl_config["table_name"])
            )

        df = raw_table.load_from_csv(
            location=str(tbl_config["stage_path"]),
            file_format=str(tbl_config["file_format"]),
            mode=WriteMode(str(tbl_config["mode"]).lower()),
            force=bool(tbl_config["force"])
            )

        # print(f"load table {str(tbl_config['table_name'])} succeeded")
        return df
    except Exception as err:
        # print(f"load table {str(tbl_config['table_name'])} failed with error {err}")
        # dg: DataFrame = None
        raise err
