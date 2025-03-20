"""
TODO :
- create resource monitors
- create domain personaes roles
"""
import os
import logging
from typing import Any
import json
import jsonschema_fill_default  # type: ignore
import yaml  # type: ignore
from jsonschema import validate  # type: ignore
from snowflake.snowpark.session import Session
from dcube.snowflake.mesh.query_plan import QueryPlan
from dcube.snowflake.mesh.mesh_role import MeshRoles
from dcube.snowflake.mesh.warehouse import Warehouses
from dcube.snowflake.mesh.database import Databases

# Get the logger
log = logging.getLogger(__name__)
# Get or create snowpark session
session: Session = Session.builder.getOrCreate()


class MeshContract:
    """
    This kind of data contract is defined into a yml file.
    If contains the differents parts used to standardize the representation
    of your data mesh on a snowflake account:
    - the mesh_admin users: list of users on which we need to grant role on
    - the data domains: list of data domains
    - the domain_multiples: list of data domain mutliple
    - the custom database roles: list of database roles
    """

    def __init__(self, mesh_contract_filepath: str) -> None:
        """
        MeshManager constructor
        Args:
        - mesh_contract_filepath: the yml file path
        """
        # set the data contract
        self.set_data_contrat(mesh_contract_filepath)

    def __normalize(self, obj: Any) -> Any:
        """
        Normalize the data contract, triming and lowwercase all keys and values
        Args:
        - obj: a data contract dictionnary
        Returns:
        - the data contract normalized
        """
        if isinstance(obj, dict):
            return {
                str(k).strip().lower():  # type: ignore
                self.__normalize(v)
                for k, v in obj.items()  # type: ignore
            }
        elif isinstance(obj, list):
            return [self.__normalize(v) for v in obj]  # type: ignore
        elif isinstance(obj, str):
            return obj.strip().lower()
        else:
            return obj  # Return the object as-is for non-string types

    def __validate_data_contract(self,
                                 instance: dict[str, Any]) -> dict[str, Any]:
        """
        Read, parse, validate the data_contract
        Args:
        - instance dict[Any, Any]: th data contract dictionnary to validate
        Returns:
        - the normalized data contract controlled and enriched with default values
        """
        with open(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "mesh_contract.json"
                ),
                "r") as cntrct_sch_raw_content:
            data_contract_sch = json.load(cntrct_sch_raw_content)

        # normalize all the key-pair values
        data_contract = self.__normalize(instance)

        # fill the data_contract with missing default values
        jsonschema_fill_default.fill_default(  # type: ignore
            instance=data_contract, schema=data_contract_sch)

        # validate the data contract using the jsonschema.validate method
        validate(instance=data_contract, schema=data_contract_sch)

        return data_contract.get("data_contract")

    def set_data_contrat(self, mesh_contract_filepath: str) -> None:
        """
        Load the data contract from the project_config.yml config file
        Args:
        - - mesh_contract_filepath: the yml file path
        """
        # read, parse and normalize the data contract yaml file
        data_contract = self.__validate_data_contract(
            instance=yaml.safe_load(open(mesh_contract_filepath, "rb")))

        # parse the data contract and init class properties
        self._version: str = data_contract.get("version", "")
        self._kind: str = data_contract.get("kind", "")
        self._provider: str = data_contract.get("provider", "")

        # mesh_roles
        self._mesh_roles = MeshRoles(data_contract)

        # warehouses
        self._warehouses = Warehouses(data_contract)

        # databases
        self._databases = Databases(data_contract)

    def plan(self) -> QueryPlan:
        """
        Generate the sql statements to manage the data mesh databases domain
        and access management policies.
        """
        # init the query plan
        qp = QueryPlan()

        qp.add_blocks(self._mesh_roles.plan().get_blocks() +
                      self._warehouses.plan().get_blocks() +
                      self._databases.plan().get_blocks())

        # old code
        # drop current custom database roles missing from the data contract
        # qp.add_blocks(self.__plan_drop_unref_databases_roles().get_blocks())

        return qp
