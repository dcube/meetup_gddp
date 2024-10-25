"""..."""
import os
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.context import get_active_session
import streamlit as st


@st.cache_resource
def get_or_init_session() -> Session:
    """..."""
    session: Session
    try:
        # get active session
        session = get_active_session()
    except SnowparkSessionException:
        #
        session = Session.builder.configs(
            {
                "account":  os.getenv("SNOWFLAKE_ACCOUNT"),
                "warehouse": os.getenv("SNOWFLAKE_STREAMLIT_WAREHOUSE"),
                "role": os.getenv("SNOWFLAKE_STREAMLIT_ROLE"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD")
            }
            ).create()
    return session
