"""..."""
from typing import Dict, Any
from snowflake.snowpark import Session, DataFrame


class RawDataCSVLoader():
    """Class to ease raw csv data loading from stage into snowflake table"""
    __session: Session
    __config: Dict[str, Any]
    __config_validator: Dict[str, Any] = {
        "table_name": (str, None),
        "location": (str, None),
        "file_format": (str, None),
        "infer_schema_max_file_count": (int, None),
        "infer_schema_max_records_per_file": (int, None),
        "ignore_case": (bool, None),
        "enable_schema_evolution": (bool, None),
        "write_mode": (str, ["append", "overwrite"]),
        "match_by_column_name": (str, ["case_insensitive", "case_sensitive"]),
        "force": (bool, None)
    }

    def __init__(self, session: Session, config: Dict[str, Any]) -> None:
        """default class constructor"""
        # Ensure the config dictionary has the correct structure and types
        self.__validate_config(config)
        # If validation passes, assign it to the private class attribute
        self.__config = config
        # assign snowpark serssion to the private class attribute
        self.__session = session

    def __validate_config(self, config: Dict[str, Any]) -> None:
        """validate the config used to load the data """
        # Loop through the keys in the validator
        for key, (expected_type, accepted_values) in self.__config_validator.items():
            # Check if the key exists in the config
            if key not in config:
                raise ValueError(f"Missing required config key: '{key}'")
            # Check if the value is of the expected type
            if not isinstance(config[key], expected_type):
                raise TypeError(f"Invalid type for key '{key}': expected {expected_type.__name__}, got {type(config[key]).__name__}")
            # If there are valid values specified, ensure the value is in that set
            if accepted_values and config[key] not in accepted_values:
                raise ValueError(f"Invalid value for '{key}': must be one of {accepted_values}")

    def __create_table_if_not_exists(self) -> None:
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

    def __copy_raw_data(self) -> DataFrame:
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

    def load_raw_data(self) -> DataFrame:
        """entire process to load raw data"""
        # create table if not exists
        self.__create_table_if_not_exists()

        # truncate table if mode is owervrite
        if self.__config["write_mode"] == "overwrite":
            self.__session.table(self.__config["table_name"]).delete()

        # copy raw data from stage into snowflake table
        df = self.__copy_raw_data()

        return df


def load_raw_data(session: Session, config: Dict[str, Any]) -> DataFrame:
    """entry point for stored procedure to load csv raw data"""
    csv_data_loader = RawDataCSVLoader(session, config)
    return csv_data_loader.load_raw_data()
