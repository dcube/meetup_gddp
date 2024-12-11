from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException


def table_exists(session: Session, tbl_name: str) -> bool:
    """ try to get schema from table """
    df = session.table(tbl_name)
    try:
        # if dataframe has schema => table exists
        _ = df.schema
        return True
    except SnowparkSQLException:
        # object doesn't exist
        return False


def load_csv(session: Session, tbl_name: str, fmt_name: str, location: str) -> DataFrame:
    # read from external stage with inference
    df = (
        session.read
        .option("format_name", fmt_name)
        .csv(location)
    )

    # save the data frame as a table
    df.write.mode(
        "overwrite" if not table_exists(session, tbl_name) else "truncate"
    ).save_as_table(tbl_name)

    # Return value will appear in the Results tab.
    return df


def test_load_csv(session: Session) -> None:
    """test the load function"""
    df = load_csv(
        session=session,
        tbl_name='MEETUP_GDDP.TPCH_SF100.REGION_TEST2',
        fmt_name='MEETUP_GDDP.UTILS.CSV_FMT1',
        location='@MEETUP_GDDP.UTILS.LANDING/tpch-sf100/csv/region/'
    )

    # 5 rows should be loaded
    assert df.count()==5
