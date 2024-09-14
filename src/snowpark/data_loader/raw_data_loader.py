"""..."""
from typing import Dict, Any
from abc import ABC, abstractmethod
from snowflake.snowpark import Session, DataFrame


class RawDataLoader(ABC):
    """abstract class to load any king of raw data"""
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
        self._validate_config(config)
        # If validation passes, assign it to the private class attribute
        self.__config = config
        # assign snowpark serssion to the private class attribute
        self.__session = session

    def _validate_config(self, config: Dict[str, Any]) -> None:
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

    @abstractmethod
    def _create_table_if_not_exists(self) -> None:
        """create table, if not exists, with schema inference and shema drifting"""
        # Nothing to do

    @abstractmethod
    def _copy_raw_data(self) -> DataFrame:
        """copy raw data from stage into snowflake table"""
        # Nothing to do

    def load_raw_data(self) -> DataFrame:
        """entire process to load raw data"""
        # create table if not exists
        self._create_table_if_not_exists()

        # truncate table if mode is owervrite
        if self.__config["write_mode"] == "overwrite":
            self.__session.table(self.__config["table_name"]).delete()

        # copy raw data from stage into snowflake table
        df = self._copy_raw_data()

        return df
