#------------------------------------------------------------------------------
# Product: Meetup GDDP
# Script:       load_raw_csv.py
# Author:       Frédéric BROSSARD
# Last Updated: 16/09/2024
#------------------------------------------------------------------------------
from typing import Dict, Any
from snowflake.snowpark import Session


def load_raw_csv(session: Session, db_schema: str, table: str, location: str, format_options: Dict[str, Any]) -> None:
    """Read csv and load into table"""
    session.use_schema(db_schema)

    # read csv from stage with schema inference
    df = session.read.options(format_options).csv(f"@{location}")

    # write to table
    df.write.mode("truncate").save_as_table(f"{table}")


# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as snowpark_session:
        snowpark_session.use_warehouse("LOAD")

        load_raw_csv(
          session=snowpark_session,
          db_schema="MEETUP_GDDP.TPCH_SF100",
          table="REGION",
          location="MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/region/",
          format_options={
                  "field_delimiter": "|",
                  "field_optionally_enclosed_by": "\"",
                  "parse_header": True,
                  "date_format": "YYYY-MM-DD",
                  "skip_blank_lines": True,
                  "infer_schema": True
                }
          )
