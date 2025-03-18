"""...
    List of schema object privileges.
    Do I need to create an enum class for that ?
    "CREATE ALERT"
    "MODIFY"
    "MONITOR"
    "USAGE"
    "CREATE ALERT"
    "CREATE CORTEX SEARCH SERVICE"
    "CREATE DATA METRIC FUNCTION"
    "CREATE DATASET"
    "CREATE EVENT TABLE"
    "CREATE FILE FORMAT"
    "CREATE FUNCTION"
    "CREATE GIT REPOSITORY"
    "CREATE IMAGE REPOSITORY"
    "CREATE MODEL"
    "CREATE NETWORK RULE"
    "CREATE NOTEBOOK"
    "CREATE PIPE"
    "CREATE PROCEDURE"
    "CREATE AGGREGATION POLICY"
    "CREATE AUTHENTICATION POLICY"
    "CREATE MASKING POLICY"
    "CREATE PACKAGES POLICY"
    "CREATE PASSWORD POLICY"
    "CREATE PRIVACY POLICY"
    "CREATE PROJECTION POLICY"
    "CREATE ROW ACCESS POLICY"
    "CREATE SESSION POLICY"
    "CREATE SECRET"
    "CREATE SEQUENCE"
    "CREATE SERVICE"
    "CREATE SNAPSHOT"
    "CREATE STAGE"
    "CREATE STREAM"
    "CREATE STREAMLIT"
    "CREATE SNOWFLAKE.CORE.BUDGET"
    "CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE"
    "CREATE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER"
    "CREATE SNOWFLAKE.ML.ANOMALY_DETECTION"
    "CREATE SNOWFLAKE.ML.CLASSIFICATION"
    "CREATE SNOWFLAKE.ML.FORECAST"
    "CREATE SNOWFLAKE.ML.TOP_INSIGHTS"
    "CREATE SNOWFLAKE.ML.DOCUMENT_INTELLIGENCE"
    "CREATE TABLE"
    "CREATE DYNAMIC TABLE"
    "CREATE EXTERNAL TABLE"
    "CREATE ICEBERG TABLE"
    "CREATE TAG"
    "CREATE TASK"
    "CREATE VIEW"
    "CREATE MATERIALIZED VIEW"
"""


class SchemaObjectPrivileges:
    """
    A database schema object type and its privileges
    """
    def __init__(self, name: str, privileges: list[str]) -> None:
        self._name = name
        self._privileges = privileges

    def get_name(self) -> str:
        """
        Get the database schema type
        """
        return self._name

    def get_privileges(self) -> list[str]:
        """
        Get the list of prileges on a database schema
        """
        return self._privileges
