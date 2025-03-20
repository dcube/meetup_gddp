"""..."""
from typing import Any
from dcube.snowflake.mesh.base_object import BaseObject
from dcube.snowflake.mesh.query_plan import QueryPlan, QueryPlanBlock


class Warehouse(BaseObject):
    """
    A warehouse
    """

    def __init__(self, data: dict[str, Any]) -> None:
        super().__init__(name=data.get("name", ""),
                         comment=data.get("comment", ""))

        self._warehouse_type: str = data.get("warehouse_type", "standard")
        self._warehouse_size: str = data.get("warehouse_size", "xsmall")
        self._min_cluster_count: int = data.get("min_cluster_count", 1)
        self._max_cluster_count: int = data.get("max_cluster_count", 1)
        self._scaling_policy: str = data.get("scaling_policy", "standard")
        self._auto_suspend: int = data.get("auto_suspend", 60)
        self._auto_resume: bool = data.get("auto_resume", True)
        self._initially_suspended: bool = data.get("initially_suspended", True)
        self._resource_monitor: str = data.get("resource_monitor", "")
        self._enable_query_acceleration: bool = data.get(
            "enable_query_acceleration", False)
        self._query_acceleration_max_scale_factor: int = data.get(
            "query_acceleration_max_scale_factor", 0)
        self._resource_constraint: str = data.get("resource_constraint", "")

    def get_property(self, property_name: str) -> Any:
        """
        Generic getter for warehouse properties
        """
        return getattr(self, f"_{property_name}")

    def plan_create_or_alter(self) -> str:
        """
        Plan the create or alter statement
        """
        sql = f"""
create or alter warehouse {self.get_name()}
comment = '{self.get_comment()}'
warehouse_size = '{self.get_property("warehouse_size")}'
warehouse_type = '{self.get_property("warehouse_type")}'
min_cluster_count = {self.get_property("min_cluster_count")}
max_cluster_count = {self.get_property("max_cluster_count")}
scaling_policy = '{self.get_property("scaling_policy")}'
auto_suspend = {self.get_property("auto_suspend")}
auto_resume = {self.get_property("auto_resume")}
initially_suspended = {self.get_property("initially_suspended")}
enable_query_acceleration = {self.get_property("enable_query_acceleration")}
"""
        if self.get_property("enable_query_acceleration"):
            sql += "\nquery_acceleration_max_concurrency_scaling_factor = %s" % self.get_property(
                "query_acceleration_max_scale_factor")

        if self.get_property("resource_monitor"):
            sql += "\nresource_monitor = '%s'\n" % self.get_property(
                "resource_monitor")

        if self.get_property("resource_constraint"):
            sql += "\nresource_constraint = '%s'\n" % self.get_property(
                "resource_constraint")

        return sql


class Warehouses:

    def __init__(self, data_contract: dict[Any, Any]) -> None:
        self._warehouses: list[Warehouse] = [
            Warehouse(w) for w in data_contract.get("warehouses", {})
        ]

    def plan(self) -> QueryPlan:
        """
        Generate quarry plan to manage warehouses
        """
        # init a query plan
        qp = QueryPlan()

        qp.add_blocks([
            QueryPlanBlock(name="create_or_alter_warehouses",
                           role_to_use="sysadmin",
                           parallel_mode=True,
                           sql_statements=[
                               w.plan_create_or_alter()
                               for w in self._warehouses
                           ])
        ])

        return qp
