# type: ignore
"""..."""
import streamlit as st
from modules.st_utils.page_template import PageTemplate


class HomePage(PageTemplate):
    """ Home page"""

    @classmethod
    def __init__(cls):
        """ class constructor """
        # Call the PageTemplace class constructor
        super().__init__()

    @classmethod
    def display(cls) -> None:
        """ displaying this page by override the Template"""
        st.markdown(
            "<h1 style='text-align: center;'>TPC-H Data App</h1>",
            unsafe_allow_html=True)


if __name__ == '__main__':
    # set home page properties
    my_page = HomePage()

    # display the page
    my_page.display()
