"""
Module: dcube.snowflake.deployer
Script: tool.py
Author: Frédéric BROSSARD
Last Updated: 16/09/2024
"""
# pylint: disable=W0212
# pyright: reportPrivateUsage=false
import os
import re
import logging


def replace_env_vars(value: str) -> str:
    """Function to replace environment variable placeholders in a string"""
    # Configure logging to output to stdout
    pattern = re.compile(r'&\{(\w+)\}')
    matches = pattern.findall(value)
    res = value
    for match in matches:
        env_value = os.getenv(match, f"&{{{match}}}")
        res = value.replace(f"&{{{match}}}", env_value)
        logging.debug("replacement done for %s in %s", match, value)
    return res


def get_all_yml_files(directory: str) -> list[str]:
    """Function to get all .yml files in a directory recursively"""
    yml_files: list[str] = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".yml"):
                yml_files.append(os.path.join(root, file))
                logging.debug("yml file %s found", yml_files)
    return yml_files


def text_shortener(text: str, max_length: int, fill_char: str) -> str:
    """ print query history from the session """
    shortened = re.sub(r"\s+",
                       " ",
                       re.sub(r"[\t\n\r]",
                              "",
                              text
                              )
                       )[:max_length].ljust(max_length, fill_char)
    return shortened
