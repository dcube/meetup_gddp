"""..."""
from typing import Dict
from snowflake.snowpark.dataframe import DataFrame


def uppercase_all_column_names(df: DataFrame) -> DataFrame:
    """transform to uppercase all column names of a DataFrame"""
    df_transformed = df.select(*[df[col].alias(col.upper())  # type: ignore
                                 for col in df.columns])
    return df_transformed


def lowercase_all_column_names(df: DataFrame) -> DataFrame:
    """transform lowercase all column names of a DataFrame"""
    df_transformed = df.select(*[df[col].alias(col.lower())  # type: ignore
                                 for col in df.columns])
    return df_transformed


def select_and_alias_columns(df: DataFrame, cols: Dict[str, str]) -> DataFrame:
    """select and rename the columns from a dataframe according
    to the cols key/pair value parameter"""
    df_transformed = df.select(*[df[col].alias(alias)  # type: ignore
                                 for col, alias in cols.items()])
    return df_transformed
