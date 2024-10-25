"""..."""
from snowflake.snowpark.session import Session
from sf_utils.session import get_or_init_session
import streamlit as st


class PageTemplate:
    """class to manage common page ressources"""

    # page settings
    sf_session: Session

    def __init__(self) -> None:
        """ page template constructor"""
        # Page layout config
        st.set_page_config(
            initial_sidebar_state='auto',
            layout='wide'
            )

        # get or init snowpark_session
        self.sf_session = get_or_init_session()

        # keep session between page navigation
        st.session_state.update(st.session_state)
