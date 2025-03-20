from typing import Any
import logging
from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from dcube.snowflake.mesh.base_object import BaseObject
from dcube.snowflake.mesh.query_plan import QueryPlan, QueryPlanBlock
from dcube.snowflake.mesh.database_role import DatabaseRole
from dcube.snowflake.mesh.database_role_object_privileges import DatabaseRoleObjectPrivileges as ObjectPrivileges
from dcube.snowflake.mesh.privilege_enum import PrivilegeAction
from concurrent.futures import ThreadPoolExecutor

# Get or create snowpark session
session: Session = Session.builder.getOrCreate()
# Get the logger
log = logging.getLogger(__name__)


class Database(BaseObject):

    def __init__(self, name: str, comment: str, domain: str) -> None:
        self._name = name
        self._comment = comment
        self._domain = domain

    def get_name(self) -> str:
        """
        Get the name of the database.
        """
        return self._name

    def get_domain(self) -> str:
        """
        Get the domain of the database.
        """
        return self._domain

    def plan_db_create_or_alter(self) -> str:
        """
        Plan the create or alter statement for the database.
        """
        sql = "create or alter database %s comment = '%s'" % (
            self._name,
            self._comment.replace("'", "''"))
        return sql

class Databases:
    def __init__(self, data_contract: dict[Any, Any]) -> None:
        self._domains: list[str] = [d.get("name") for d in data_contract.get("domains", {})]
        self._multiples: list[str] = [dm.get("name") for dm in data_contract.get("domain_multiples", {})]
        self._databases: list[Database] = [
            Database(
                name=f"{dm}_{d}",
                comment=f"database for domain {d} related {dm}",
                domain=d
            )
            for d in self._domains
            for dm in self._multiples
        ]

        # get the list of managed database role schema objects
        mngd_db_role_sch_objs: list[str] = data_contract.get("managed_db_role_schema_objects", [])

        # set the custom database roles for the role "admin" for schemas and objects ownership
        self._custom_db_roles: list[DatabaseRole] = [
            DatabaseRole(
                name="admin",
                comment="for admin",
                objects_privileges=[
                    ObjectPrivileges(
                        object_type="schemas",
                        privileges=["ownership"]
                    )
                ] + [
                    ObjectPrivileges(
                        object_type=o,
                        privileges=["ownership"]
                    )
                    for o in mngd_db_role_sch_objs
                ]
            )
        ]


        # add the custom database roles
        for dbr in data_contract.get("database_roles", []):
            self._custom_db_roles.append(
                DatabaseRole(
                    name=dbr.get("name"),
                    comment=dbr.get("comment"),
                    objects_privileges=[
                        ObjectPrivileges(
                            object_type=k,
                            privileges=v
                        )
                        for k, v in dbr.get("schema_objects_privileges", []).items()
                        if k in mngd_db_role_sch_objs and k != "admin" # keep only managed schema objects
                    ] + [
                        ObjectPrivileges(
                            object_type="schemas",
                            privileges=dbr.get("schemas_privileges", [])
                        )
                    ]
                )
            )

    def plan_drop_unref_databases_roles(self) -> list[str]:
        """
        Generate sql statements to drop custom database roles
        missing from the data contract
        Returns:
        - a list of SQL statements
        """
        # List of custom database roles from the data contract
        roles = {cdr.get_name() for cdr in self._custom_db_roles}

        # Loop over domains and multiples to get actual db roles
        # => need accountadmin role to avoid roles misconception
        current_role = session.get_current_role()
        session.use_role("accountadmin")

        # init the sql statements as an empty array
        sqls: list[str] = []

        # query snowflake databases and check if there're unreferenced database rols
        def process_db(db: Database) -> list[str]:
            partial_sqls: list[str] = []
            try:
                df: DataFrame = session.sql(  # type: ignore
                    "show database roles in %s" % db.get_name())
                for row in df.collect():  # type: ignore
                    role = str(row["name"]).lower()  # type: ignore
                    if role != "admin" and role not in roles:
                        partial_sqls.append("drop database role %s.%s" % (db.get_name(), role))
            except SnowparkSQLException as err:
                if (err.error_code == "1304" and err.raw_message
                        == "SQL compilation error:\nDatabase '%s' does not exist or not authorized." % db.get_name()):
                    log.error(
                        str(err.raw_message).replace("SQL compilation error:\n", "")
                    )
            return partial_sqls

        with ThreadPoolExecutor() as executor:
            results = executor.map(process_db, self._databases)
            for res in results:
                sqls.extend(res)

        # reset back the current role
        if current_role:
            session.use_role(current_role)

        return sqls

    def plan(self) -> QueryPlan:
        """
        Generate query plan to manage databases
        """
        # init a query plan
        qp: QueryPlan = QueryPlan()

        # create or alter databases
        qp.add_block(
            QueryPlanBlock(
                name="create_or_alter_dbs",
                role_to_use="sysadmin",
                parallel_mode=True,
                sql_statements=[
                    d.plan_db_create_or_alter()
                    for d in self._databases
                ]
            )
        )

        # create or alter custom database roles
        qp.add_block(
            QueryPlanBlock(
                name="create_or_alter_dbs_roles",
                role_to_use="sysadmin",
                parallel_mode=True,
                sql_statements=[
                    cdr.plan_create_or_alter(
                        database_name=d.get_name())
                    for d in self._databases
                    for cdr in self._custom_db_roles
                ]
            )
        )

        # grant custom database roles to domain admin
        qp.add_block(
            QueryPlanBlock(
                name="grant_dbs_roles_to_domain_admin",
                role_to_use="securityadmin",
                parallel_mode=True,
                sql_statements=[
                    cdr.plan_grant_to_domain_admin(
                        database_name=d.get_name(),
                        domain=d.get_domain())
                    for d in self._databases
                    for cdr in self._custom_db_roles
                ]
            )
        )

        # revoke and grant custom database roles' privileges
        for action in (PrivilegeAction.REVOKE, PrivilegeAction.GRANT):
            qp.add_block(
                QueryPlanBlock(
                    name=f"{action.value}_dbs_roles_privileges",
                    role_to_use="securityadmin",
                    parallel_mode=True,
                    sql_statements=[
                        sql
                        for d in self._databases
                        for cdr in self._custom_db_roles
                        for sql in cdr.plan_objects_privileges(
                            action=action,
                            database_name=d.get_name()
                        )
                    ]
                )
            )

        # drop unreferenced database roles
        qp.add_block(
            QueryPlanBlock(
                name="drop unref databases roles",
                role_to_use="sysadmin",
                parallel_mode=True,
                sql_statements=self.plan_drop_unref_databases_roles()
            )
        )

        return qp
