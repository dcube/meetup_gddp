# type: ignore
"""..."""
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.context import get_active_session
import streamlit as st


@st.cache_resource()
def get_or_create_session() -> Session:
    """get or create a snowpark session and cache it"""
    session: Session
    try:
        # get active session
        session = get_active_session()
    except SnowparkSessionException:
        session = Session.builder.getOrCreate()
    return session


def get_session() -> Session:
    """singleton pattern to get or create a snowpark session for the user"""
    # get cached session or init it
    _sess = get_or_create_session()
    try:
        # check if session is alive
        _sess.get_current_account()
    except SnowparkSessionException:
        # if session is not alige force clear cache and recreate a session
        get_or_create_session.clear()
        _sess = get_or_create_session()
    return _sess
