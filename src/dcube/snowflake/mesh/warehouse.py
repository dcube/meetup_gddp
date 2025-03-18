"""..."""
from typing import Any
from dcube.snowflake.mesh.base_object import BaseObject


class Warehouse(BaseObject):
    """
    A warehouse
    """

    def __init__(self, data: dict[str, Any]) -> None:
        super().__init__(
            name=data.get("name", ""),
            comment=data.get("comment", "")
        )

        self._warehouse_type: str = data.get("warehouse_type", "standard")
        self._warehouse_size: str = data.get("warehouse_size", "xsmall")
        self._min_cluster_count: int = data.get("min_cluster_count", 1)
        self._max_cluster_count: int = data.get("max_cluster_count", 1)
        self._scaling_policy: str = data.get("scaling_policy", "standard")
        self._auto_suspend: int = data.get("auto_suspend", 60)
        self._auto_resume: bool = data.get("auto_resume", True)
        self._initially_suspended: bool = data.get("initially_suspended", True)
        self._resource_monitor: str = data.get("resource_monitor", "")
        self._enable_query_acceleration: bool = data.get("enable_query_acceleration", False)
        self._query_acceleration_max_scale_factor: int = data.get("query_acceleration_max_scale_factor", 0)
        self._resource_constraint: str = data.get("resource_constraint", "")

    def get_warehouse_type(self) -> str:
        """
        Get the warehouse type
        """
        return self._warehouse_type

    def get_warehouse_size(self) -> str:
        """
        Get the warehouse size
        """
        return self._warehouse_size

    def get_min_cluster_count(self) -> int:
        """
        Get the minimum cluster count
        """
        return self._min_cluster_count

    def get_max_cluster_count(self) -> int:
        """
        Get the maximum cluster count
        """
        return self._max_cluster_count

    def get_scaling_policy(self) -> str:
        """
        Get the scaling policy
        """
        return self._scaling_policy

    def get_auto_suspend(self) -> int:
        """
        Get the auto suspend time
        """
        return self._auto_suspend

    def get_auto_resume(self) -> bool:
        """
        Get the auto resume flag
        """
        return self._auto_resume

    def get_initially_suspended(self) -> bool:
        """
        Get the initially suspended flag
        """
        return self._initially_suspended

    def get_resource_monitor(self) -> str:
        """
        Get the resource monitor
        """
        return self._resource_monitor

    def get_enable_query_acceleration(self) -> bool:
        """
        Get the enable query acceleration flag
        """
        return self._enable_query_acceleration

    def get_query_acceleration_max_scale_factor(self) -> int:
        """
        Get the query acceleration max scale factor
        """
        return self._query_acceleration_max_scale_factor

    def get_resource_constraint(self) -> str:
        """
        Get the resource constraint
        """
        return self._resource_constraint

    def plan_create_or_alter(self) -> str:
        """
        Plan the create or alter statement
        """
        sql = f"""
create or alter warehouse {self.get_name()}
comment = '{self.get_comment()}'
warehouse_size = '{self.get_warehouse_size()}'
warehouse_type = '{self.get_warehouse_type()}'
min_cluster_count = {self.get_min_cluster_count()}
max_cluster_count = {self.get_max_cluster_count()}
scaling_policy = '{self.get_scaling_policy()}'
auto_suspend = {self.get_auto_suspend()}
auto_resume = {self.get_auto_resume()}
initially_suspended = {self.get_initially_suspended()}
enable_query_acceleration = {self.get_enable_query_acceleration()}
"""
        if self.get_enable_query_acceleration():
            sql += f"\nquery_acceleration_max_concurrency_scaling_factor = {self.get_query_acceleration_max_scale_factor()}"

        if self.get_resource_monitor():
            sql += f"\nresource_monitor = '{self.get_resource_monitor()}'\n"

        if self.get_resource_constraint():
            sql += f"\nresource_constraint = '{self.get_resource_constraint()}'\n"

        return sql
