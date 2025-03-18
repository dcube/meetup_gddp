"""..."""
from typing import Any
from dcube.snowflake.mesh.base_object import BaseObject
from dcube.snowflake.mesh.schema_object_privileges import SchemaObjectPrivileges

class DatabaseRole(BaseObject):
    """
    A database role
    """

    def __init__(self, data: dict[str, Any]) -> None:
        super().__init__(
            name=data.get("name", ""),
            comment=data.get("comment", "")
        )
        self._schema_privileges: list[str] = data.get("schema_privileges", [])
        sop = data.get("schema_objects_privileges")
        if sop is not None:
            self._schema_objects_privileges: list[SchemaObjectPrivileges] = [
                SchemaObjectPrivileges(name=str(k), privileges=v)
                for k, v in sop.items()
            ]

    def get_schema_privileges(self) -> list[str]:
        """
        Get the list of schema privileges
        """
        return self._schema_privileges

    def get_schema_objects_privileges(self) -> list[SchemaObjectPrivileges]:
        """
        Get the list of schema objects' privileges
        """
        return self._schema_objects_privileges
