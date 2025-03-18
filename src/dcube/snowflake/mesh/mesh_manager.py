"""
TODO :
- create resource monitors
- create warehouses
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
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from dcube.snowflake.mesh.domain import Domain
from dcube.snowflake.mesh.domain_multiple import DomainMultiple
from dcube.snowflake.mesh.database_role import DatabaseRole
from dcube.snowflake.mesh.query_plan import QueryPlan, QueryPlanBlock
from dcube.snowflake.mesh.warehouse import Warehouse

# Get the logger
log = logging.getLogger(__name__)
# Get or create snowpark session
session: Session = Session.builder.getOrCreate()


class MeshManager:
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

    def __validate_data_contract(self, instance: dict[str, Any]) -> dict[str, Any]:
        """
        Read, parse, validate the data_contract
        Args:
        - instance dict[Any, Any]: th data contract dictionnary to validate
        Returns:
        - the normalized data contract controlled and enriched with default values
        """
        with open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "mesh_manager.json"),
                "r") as cntrct_sch_raw_content:
            data_contract_sch = json.load(cntrct_sch_raw_content)

        # normalize all the key-pair values
        data_contract = self.__normalize(instance)

        # fill the data_contract with missing default values
        jsonschema_fill_default.fill_default(  # type: ignore
            instance=data_contract, schema=data_contract_sch)

        # validate the data contract using the jsonschema.validate method
        validate(instance=data_contract, schema=data_contract_sch)

        # add custom validation


        return data_contract.get("data_contract")

    def set_data_contrat(self, mesh_contract_filepath: str) -> None:
        """
        Load the data contract from the project_config.yml config file
        Args:
        - - mesh_contract_filepath: the yml file path
        """
        # read, parse and normalize the data contract yaml file
        data_contract = self.__validate_data_contract(
            instance=yaml.safe_load(
                open(mesh_contract_filepath, "rb")
                )
            )

        # parse the data contract and init class properties
        self._version: str = data_contract.get("version", "")
        self._kind: str = data_contract.get("kind", "")
        self._provider: str = data_contract.get("provider", "")
        self._mesh_admin_users: list[str] = data_contract.get(
            "mesh_admin_users", [])

        self._domains: list[Domain] = [
            Domain(d.get("name"), d.get("comment"))
            for d in data_contract.get("domains", {})
        ]

        self._domain_multiples: list[DomainMultiple] = [
            DomainMultiple(ds.get("name"), ds.get("comment"))
            for ds in data_contract.get("domain_multiples", {})
        ]

        self._mngd_db_role_sch_objs: list[str] = data_contract.get(
            "managed_db_role_schema_objects", [])

        self._db_roles: list[DatabaseRole] = [
            DatabaseRole(dr) for dr in data_contract.get("database_roles", {})
        ]

        self._warehouses: list[Warehouse] = [
            Warehouse(w) for w in data_contract.get("warehouses", {})
        ]

    def __plan_warehouses(self) -> QueryPlan:
        """
        Generate sql statements to create or alter warehouses
        """
        # init a query plan
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_sysadmin": ["use role sysadmin",],
            "warehouses": [],
        }

        # loop over domains to create role admin for all domain
        for w in self._warehouses:
            # generate block to create domain admin roles
            sql_statements["warehouses"].append(w.plan_create_or_alter())

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def __plan_mesh_admin_roles(self) -> QueryPlan:
        """
        Generate sql statements to create admin roles for all domains
        returns:
        - a query plan block with all the sql statements
        """
        # init a query plan
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_securityadmin": [
                "use role securityadmin",
                "create role if not exists mesh_admin",
                "grant role mesh_admin to role sysadmin"
            ],
            "roles": [],
            "grants": [],
        }

        # loop over domains to create role admin for all domain
        for d in self._domains:
            # generate block to create domain admin roles
            sql_statements["roles"].append(
                "create role if not exists %s_admin" % (d.get_name()))
            # generate block to greant domain admin roles to mesh admin
            sql_statements["grants"].append(
                "grant role %s_admin to role mesh_admin" % (d.get_name()))

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def __plan_databases(self) -> QueryPlan:
        """
        Generate sql statements for each domain and multiples
        returns:
        - a query plan block with all the sql statements to create
          the domain databases
        """
        # init a query plan
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_sysadmin": [
                "use role sysadmin",
            ],
            "dbroles": [],
        }

        # loop over domains and multiples to create
        # domain multiples database
        for d in self._domains:
            for s in self._domain_multiples:
                sql_statements["dbroles"].append(
                    "create database if not exists %s_%s" %
                    (s.get_name(), d.get_name()))

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def __plan_databases_admin_roles(self) -> QueryPlan:
        """
        Generate sql statements for admin database roles aka
        ownership on all schemas and all and managed schema objects.
        Returns:
        - the query plan with all the sql statement blocks to to create
          or alter the database roles "admin" and their privileges
        """
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_sysadmin": [
                "use role sysadmin",
            ],
            "dbroles": [],
            "switch_to_role_securityadmin": [
                "use role securityadmin",
            ],
            "grant_dbroles_to_domain_admin": [],
            "revoke_ownership_schemas": [],
            "grant_ownership_schemas": [],
            "grant_ownership_future_schemas": [],
            "revoke_ownership_schema_object": [],
            "grant_ownership_schema_object": [],
            "grant_ownership_future_schema_objects": []
        }

        # loop over domains and multiples
        for d in self._domains:
            for s in self._domain_multiples:
                # generate block to create databases roles "admin"
                sql_statements["dbroles"].append(
                    "create database role if not exists %s_%s.admin" %
                    (s.get_name(), d.get_name()))

                # generate block to grant the databases roles "admin"
                # to the domain admin role
                sql_statements["grant_dbroles_to_domain_admin"].append(
                    "grant database role %s_%s.admin to role %s_admin" %
                    (s.get_name(), d.get_name(), d.get_name()))

                # generate block to revoke ownership on future schemas
                # and grant ownership on all schemas
                sql_statements["revoke_ownership_schemas"].append(
                    "revoke ownership on future schemas in database %s_%s " %
                    (s.get_name(), d.get_name()) +
                    "from database role %s_%s.admin" %
                    (s.get_name(), d.get_name()))
                sql_statements["grant_ownership_schemas"].append(
                    "grant ownership on all schemas in database %s_%s " %
                    (s.get_name(), d.get_name()) +
                    "to database role %s_%s.admin copy current grants" %
                    (s.get_name(), d.get_name()))

                # generate block to grant ownership on future schemas
                sql_statements["grant_ownership_future_schemas"].append(
                    "grant ownership on future schemas in database %s_%s " %
                    (s.get_name(), d.get_name()) +
                    "to database role %s_%s.admin copy current grants" %
                    (s.get_name(), d.get_name()))

                # loop over the shema objects on which to manage ownership
                for o in self._mngd_db_role_sch_objs:
                    # generate block to revoke ownership
                    # on future schemas objects
                    # and grant ownership on all schemas objects
                    if o.lower() != "pipes":
                        # limitation Bulk grant on objects of type "pipe"
                        # to "database_role" is restricted
                        sql_statements["grant_ownership_schema_object"].append(
                            "grant ownership on all %s in database %s_%s " %
                            (o, s.get_name(), d.get_name()) +
                            "to database role %s_%s.admin copy current grants"
                            % (s.get_name(), d.get_name()))
                    sql_statements["revoke_ownership_schema_object"].append(
                        "revoke ownership on future %s in database %s_%s " %
                        (o, s.get_name(), d.get_name()) +
                        "from database role %s_%s.admin" %
                        (s.get_name(), d.get_name()))

                    # generate block to grant ownership
                    # on future schemas objects
                    sql_statements[
                        "grant_ownership_future_schema_objects"].append(
                            "grant ownership on future %s in database %s_%s " %
                            (o, s.get_name(), d.get_name()) +
                            "to database role %s_%s.admin copy current grants"
                            % (s.get_name(), d.get_name()))

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def __plan_databases_custom_roles(self) -> QueryPlan:
        """
        Generate sql statements for custom database roles
        Returns:
        - a query plan block with all the sql statement to create or alter
          the custom database roles and their privileges
        """

        # init empty query plan
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_sysadmin": [
                "use role sysadmin",
            ],
            "dbroles": [],
            "switch_to_role_securityadmin": [
                "use role securityadmin",
            ],
            "grants": [],
        }

        # loop over domains and multiples
        for d in self._domains:
            for s in self._domain_multiples:
                for r in self._db_roles:
                    # role name must not be "admin"
                    if r.get_name() != "admin":
                        # generate block to create databases roles "admin"
                        sql_statements["dbroles"].append(
                            "create or alter database role %s_%s.%s" %
                            (s.get_name(), d.get_name(), r.get_name()))

                        # generate block to grant the databases roles
                        # to the role the domain admin role
                        sql_statements["grants"].append(
                            "grant database role %s_%s.%s to role %s_admin" %
                            (s.get_name(), d.get_name(), r.get_name(),
                             d.get_name()))

                        # loop over schema privileges
                        for p in r.get_schema_privileges():
                            # privilege must not be "ownership"
                            if p != "ownership":
                                # grant the privilege on all schemas
                                sql_statements["grants"].append(
                                    "grant %s on all schemas " % p +
                                    "in database %s_%s " %
                                    (s.get_name(), d.get_name()) +
                                    "to database role %s_%s.%s " %
                                    (s.get_name(), d.get_name(), r.get_name()))
                                # grant the privilege on future schemas
                                sql_statements["grants"].append(
                                    "grant %s on future schemas " % p +
                                    "in database %s_%s " %
                                    (s.get_name(), d.get_name()) +
                                    "to database role %s_%s.%s " %
                                    (s.get_name(), d.get_name(), r.get_name()))

                        # loop over schema_objects_privileges
                        for sop in r.get_schema_objects_privileges():
                            # object must be part of _mngd_db_role_sch_obj
                            if sop.get_name() in self._mngd_db_role_sch_objs:

                                # privileges must not be "ownership"
                                prvlge_cpy = sop.get_privileges()
                                if "ownership" in prvlge_cpy:
                                    prvlge_cpy.remove("ownership")
                                p = " ,".join(prvlge_cpy)

                                # grant privileges on all objects
                                if sop.get_name() != "pipes":
                                    # limitation Bulk grant on objects of type
                                    # "pipe"to "database_role" is restricted
                                    sql_statements["grants"].append(
                                        "grant %s on all %s " %
                                        (p, sop.get_name()) +
                                        "in database %s_%s " %
                                        (s.get_name(), d.get_name()) +
                                        "to database role %s_%s.%s" %
                                        (s.get_name(), d.get_name(),
                                         r.get_name()))

                                # grant privileges on future objects
                                sql_statements["grants"].append(
                                    "grant %s on future %s " %
                                    (p, sop.get_name()) +
                                    "in database %s_%s " %
                                    (s.get_name(), d.get_name()) +
                                    "to database role %s_%s.%s" %
                                    (s.get_name(), d.get_name(), r.get_name()))

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def __plan_drop_unref_databases_roles(self) -> QueryPlan:
        """
        Generate sql statements to drop custom database roles
        missing from the data contract
        Returns:
        - a query plan block with all the SQL statements to drop custom
          roles in Snowflake DB missing from the data contract
        """
        # init empty query plan
        qp: QueryPlan = QueryPlan()

        # init sql statements blocks
        sql_statements: dict[str, list[str]] = {
            "switch_to_role_sysadmin": [
                "use role sysadmin",
            ],
            "drop_unref_db_roles": []
        }

        # List of custom database roles from the data contract
        names = [r.get_name() for r in self._db_roles]

        # Loop over domains and multiples to get actual db roles
        # => need accountadmin role to avoid roles misconception
        session.use_role("accountadmin")
        for d in self._domains:
            for s in self._domain_multiples:
                try:
                    # Get the current database role in the database
                    df: DataFrame = session.sql(  # type: ignore
                        "show database roles in %s_%s" %
                        (s.get_name(), d.get_name()))

                    # Identify database roles missing in the data contract
                    for row in df.collect():  # type: ignore
                        r = str(row["name"]).lower()  # type: ignore
                        if r != "admin" and r not in names:
                            sql_statements["drop_unref_db_roles"].append(
                                "drop database role %s_%s.%s" %
                                (s.get_name(), d.get_name(), r))
                except SnowparkSQLException as err:
                    if (err.error_code == "1304" and err.raw_message
                            == "SQL compilation error:\nDatabase '%s_%s' " %
                        (s.get_name(), d.get_name()) +
                            "does not exist or not authorized."):
                        log.error(
                            str(err.raw_message).replace(
                                "SQL compilation error:\n", ""))

        # add all blocks to the query plan
        for k, _ in sql_statements.items():
            if sql_statements[k]:
                qpb: QueryPlanBlock = QueryPlanBlock()
                qpb.set_block(
                    parallel_mode=(False if k.startswith("switch_to_role_")
                                   else True),
                    sql_statements=sql_statements[k])
                qp.add_block(qpb)

        return qp

    def plan(self) -> QueryPlan:
        """
        Generate the sql statements to manage the data mesh databases domain
        and access management policies.
        """
        # init the query plan
        qp = QueryPlan()

        qp.add_blocks(self.__plan_warehouses().get_blocks())

        # create domain roles and manage their database roles grants
        qp.add_blocks(self.__plan_mesh_admin_roles().get_blocks())

        # databases objects
        qp.add_blocks(self.__plan_databases().get_blocks())

        # databases' roles "admin"
        qp.add_blocks(self.__plan_databases_admin_roles().get_blocks())

        # custom database roles
        qp.add_blocks(self.__plan_databases_custom_roles().get_blocks())

        # drop current custom database roles missing from the data contract
        qp.add_blocks(self.__plan_drop_unref_databases_roles().get_blocks())

        return qp
