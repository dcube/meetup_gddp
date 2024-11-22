"""..."""
from snowflake.snowpark.session import Session
import streamlit as st
from modules.sf_utils.session import get_session


class PageTemplate:
    """class to manage common page ressources"""

    # page settings
    _sf_session: Session

    @classmethod
    def __init__(cls) -> None:
        """ page template constructor"""
        # Page layout config
        st.set_page_config(
            initial_sidebar_state='auto',
            layout='wide'
            )

        # get or init snowpark_session
        cls.set_session()

        # set default warehouse to use
        cls._sf_session.use_warehouse("MANAGE")

        # keep session between page navigation
        st.session_state.update(st.session_state)

    @classmethod
    def set_session(cls) -> None:
        """ set snowpark session """
        cls._sf_session = get_session()

    @classmethod
    def get_session(cls) -> Session:
        """ get snowpark session"""
        return cls._sf_session
