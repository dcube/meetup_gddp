from typing import Any
from dcube.snowflake.mesh.query_plan import QueryPlan, QueryPlanBlock

class MeshRoles:

    def __init__(self, data_contract: dict[Any, Any]) -> None:
        self._domains: list[str] = [d.get("name") for d in data_contract.get("domains", {})]

    def get_domains(self) -> list[str]:
        """
        Get the list of domains
        """
        return self._domains

    def plan(self) -> QueryPlan:
        """
        Generate query plan to manage mesh roles
        """
        # init a query plan
        qp: QueryPlan = QueryPlan()
        qp.add_blocks([
            QueryPlanBlock(name="create_or_alter_mesh_admin_roles",
                            role_to_use="securityadmin",
                            parallel_mode=True,
                            sql_statements=["create or alter role mesh_admin",]),
            QueryPlanBlock(name="grant_mesh_admin_roles",
                            role_to_use="securityadmin",
                            parallel_mode=True,
                            sql_statements=["grant role mesh_admin to role sysadmin",]),])

        # loop over domains to create role admin for all domain
        for d in self._domains:
            # generate block to create or alter domain admin roles
            qp.add_block_sql_statement(block_name="create_or_alter_mesh_admin_roles", sql=f"create or alter role {d}_admin")
            # generate block to grant domain admin roles to mesh admin
            qp.add_block_sql_statement(block_name="grant_mesh_admin_roles", sql=f"grant role {d}_admin to role mesh_admin")

        return qp
