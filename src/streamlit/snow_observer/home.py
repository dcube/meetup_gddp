"""..."""
import streamlit as st
from st_utils.page_template import PageTemplate
from sf_utils.session import get_or_init_session


class HomePage(PageTemplate):
    """ Home page"""

    def render(self) -> None:
        """ rendering this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center; color: #1489a6;'>Sn❄w ❄bserver</h1>",
            unsafe_allow_html=True)


if __name__ == '__main__':
    # set home page properties
    my_page = HomePage()

    # render the homepage
    my_page.render()
