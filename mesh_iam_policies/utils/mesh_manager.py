"""..."""
import os
import logging
import re
import yaml  # type: ignore
from typing import Any
from jinja2 import Environment, FileSystemLoader
from utils.dict_utils import DictUtils  # type: ignore

# Get the logger
log = logging.getLogger(__name__)


class MeshManager():
    """
    The MeshManager class is used to administration your data mesh
    storage urbanization and role base access control policies.
    The data contract is defined into the project_config.yml file
    where are defined:
    - the data domains: a dictionnary that list your data domains
    - the environments: a dictionnary that list your environments
    - the custom database roles
    - the folder which contains the sql jinja template files
    Example:

    Todo:
        - check all the data contract not only the first level
        - remove the database roles on the env-domain database
          that are not listed into the data contract
        - add a function to normalize some data contract dict key values
          for example trim and lower the strings,
          replace spaces by _ for some key values
        - update jinja the database_role_custom to ignore env
          that are not in the environments list, same with the privileges,
          their must be into the managed_db_role_privieges list
          or create a full data contract checker to list all errors....
        - add apply method to run the compiled queries, all sql command
          which are encapsulate between start/end parallel blocks
          must be run in parallel
    """
    DATA_CONTRATCT_REQUIRED_KEYS: dict[str, Any] = {
        "kind": {
            "type": str,
            "accepted_values": [
                "mesh_manager",
            ]
        },
        "version": {
            "type": str,
            "accepted_values": [
                "0.1.0",
            ]
        },
        "provider": {
            "type": str,
            "accepted_values": [
                "snowflake",
            ]
        },
        "domains": {
            "type": list
        },
        "environments": {
            "type": list
        },
        "database_roles": {
            "type": list
        },
        "managed_db_role_schema_objects": {
            "type": list
        }
    }

    def __init__(self, project_path: str) -> None:
        """
        MeshManager constructor
        Args:
            - project_path: the project's path
        """
        # the script working dir
        self.__project_path = project_path

        # set the data contract
        self.__data_contract: dict[str, Any] = dict()
        self.set_data_contrat()

    def set_data_contrat(self) -> None:
        """
        load the data contract from the project_config.yml config file
        """

        def normalize_data_contract(obj: Any) -> Any:
            """
            normalize the data contract, triming and lowwercase
            all keys and values
            Args:
                - obj: a data contract dictionnary
            Returns
                the data contract normalized
            """
            if isinstance(obj, dict):
                return {
                    str(k).strip().lower():  # type: ignore
                    normalize_data_contract(v)
                    for k, v in obj.items()  # type: ignore
                }
            elif isinstance(obj, list):
                return [normalize_data_contract(v)
                        for v in obj]  # type: ignore
            elif isinstance(obj, str):
                return obj.strip().lower()
            else:
                return obj  # Return the object as-is for non-string types

        log.info("setting the data contract has started")
        # path for the project_config.yml which define the data contract
        data_contract_yml_file = os.path.join(self.__project_path,
                                              "project_config.yml")
        log.info(data_contract_yml_file)
        file_content = open(data_contract_yml_file, "rb")

        # parse the yaml file and get the data_contract key
        data_contract = dict(yaml.safe_load(file_content)).get(
            "data_contract", dict())

        # raise error if data_contract is missing
        if not data_contract:
            raise ValueError("'data_contract' key is missing in file %s" %
                             data_contract_yml_file)

        # normalize the data contract
        data_contract = normalize_data_contract(data_contract)

        # check the data contract
        DictUtils.check_required_keys(data_contract,
                                      self.DATA_CONTRATCT_REQUIRED_KEYS)

        # set the __data_contract_dict class attribute
        self.__data_contract = data_contract

        log.info("setting the data contract has ended")

    def get_data_contrat(self) -> dict[str, Any]:
        return self.__data_contract

    def compile(self) -> None:
        """
        Generate the sql statements to manage the data mesh databases domain
        and access management policies.
        """

        def save_to_file(content: str, folder_path: str,
                         file_name: str) -> None:
            """
            Saves the content of a variable to a file.
            It creates the folder is if it doesn't exist.
            The file is overwritten if it already exists
            """
            # Create the folder if it doesn't exist
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # Open the file in write mode (overwrite mode)
            with open(os.path.join(folder_path, file_name), 'w') as file:
                # Write the variable content to the file
                file.write(content)

        log.info("compile has started")
        log.info(f"project path is {self.__project_path}")
        log.info("data contract definition is in %s" %
                 os.path.join(self.__project_path, "project_config.yml"))

        # check if the that contract is not empty
        if not self.__data_contract:
            err_msg = "data contrat dictionnary is empty"
            log.error(err_msg)
            raise ValueError(err_msg)
        log.debug(self.__data_contract)

        # get the sql template dir
        sql_template_dir = os.path.join(
            self.__project_path,
            self.__data_contract.get('sql_template_folder', ''))

        log.info(f"sql jinja template dir is {sql_template_dir}")

        # Combine data into a single context
        jinja_context = {
            "domains":
            self.__data_contract.get("domains"),
            "environments":
            self.__data_contract.get("environments"),
            "database_roles":
            self.__data_contract.get("database_roles"),
            "managed_db_role_schema_objects":
            self.__data_contract.get("managed_db_role_schema_objects")
        }
        log.info("jinja context is set")

        # Initialize Jinja2 environment
        env = Environment(loader=FileSystemLoader(sql_template_dir))

        # loop onto the template folder to get sql files
        # and generate the sql statements
        sql_stmt: str = ""
        for sql_file in [
                f for f in os.listdir(sql_template_dir) if f.endswith('.sql')
        ]:
            sql_stmt += env.get_template(sql_file).render(jinja_context)
            log.info("jinja template file %s processed" % sql_file)

        # raise an error if no sql statement is generated
        if not sql_stmt:
            raise ValueError("compilation return empty sql statement")

        # save the sql statement into the target/compiled.sql
        save_to_file(sql_stmt, os.path.join(self.__project_path, "target"),
                     "compiled.sql")
        log.info("generated sql statements avaiable at %s" % os.path.join(
            os.path.join(self.__project_path, "target"), "compiled.sql"))

        log.info("compile has ended")

    def apply(self) -> None:
        """
        run the target/compiled.sql
        """

        def query_parser(sql_text: str) -> list[str]:
            """parse a string which contains queries separated by ;
            and return a list of query string"""
            queries: list[str] = []

            if sql_text:
                comment_pattern = re.compile(
                    r"""
                        (?:'[^'\\]*(?:\\.[^'\\]*)*') |  # Single quoted strings
                        (?:"[^"\\]*(?:\\.[^"\\]*)*") |  # Double quoted strings
                        (?:--.*?$) |  # Single-line comments
                        (?:\/\/.*?$) |  # Single-line comments with //
                        (?:\/\*.*?\*\/)  # Multi-line comments
                    """, re.VERBOSE | re.DOTALL | re.MULTILINE)

                # Function to preserve quoted strings
                def preserve_quoted_strings(match):  # type: ignore
                    if match.group(0).startswith(('--', '/*', '//')):
                        return ''  # Remove comments
                    return match.group(0)  # type: ignore

                # Remove comments from the SQL text
                clean_sql_text = re.sub(
                    comment_pattern,  # type: ignore
                    preserve_quoted_strings,  # type: ignore
                    sql_text)

                # Split the cleaned SQL text into individual statements
                statements = re.split(r';\s*', clean_sql_text)

                # Remove any empty statements
                queries = [stmt for stmt in statements if stmt]

            return queries

        log.info("apply has started")
        self.compile()

        # Open the file in write mode (overwrite mode)
        file_content = open(
            os.path.join(os.path.join(self.__project_path, "target"),
                         "compiled.sql"), "r").read()
        log.debug(query_parser(file_content))

        log.info("apply has ended")
