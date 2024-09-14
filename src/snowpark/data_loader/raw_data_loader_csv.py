"""..."""
from typing import Dict, Any
from snowflake.snowpark import Session, DataFrame
from .raw_data_loader import RawDataLoader


class RawDataLoaderCSV(RawDataLoader):
    """Class to ease raw csv data loading from stage into snowflake table"""

    def _create_table_if_not_exists(self) -> None:
        """create table, if not exists, with schema inference and shema drifting"""
        self.__session.sql(f"""
            create table if not exists { self.__config["table_name"] }
            using template (
                select array_agg(object_construct('COLUMN_NAME', column_name, 'TYPE', type, 'NULLABLE', nullable))
                from
                    table(
                        infer_schema(
                        location => '{ self.__config["location"] }',
                        file_format => '{ self.__config["file_format"] }',
                        ignore_case => { self.__config["ignore_case"] },
                        max_file_count => { self.__config["infer_schema_max_file_count"] },
                        max_records_per_file => { self.__config["infer_schema_max_records_per_file"] }
                        )
                    )
                )
            with
                enable_schema_evolution = { self.__config["enable_schema_evolution"] }
            """).collect()

    def _copy_raw_data(self) -> DataFrame:
        """copy raw data from stage into snowflake table"""

        # copy raw data from stage
        df = self.__session.sql(f"""
            copy into { self.__config["table_name"] }
            from { self.__config["location"] }
            file_format = (
                format_name = '{ self.__config["file_format"] }'
            )
            match_by_column_name = { self.__config["match_by_column_name"] }
            force = { self.__config["force"] }
            """)

        return df


def load_raw_data_csv(session: Session, config: Dict[str, Any]) -> DataFrame:
    """entry point for stored procedure to load csv raw data"""
    csv_data_loader = RawDataLoaderCSV(session, config)
    return csv_data_loader.load_raw_data()
