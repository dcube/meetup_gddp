"""..."""
from dcube.snowflake.mesh.privilege_enum import PrivilegeAction, PrivilegeOption

class DatabaseRoleObjectPrivileges:
    """
    An object type and its privileges
    """

    def __init__(self, object_type: str, privileges: list[str]) -> None:
        self._object_type = object_type
        self._privileges = list(set(privileges))

    def get_object_type(self) -> str:
        """
        Get the object type
        """
        return self._object_type

    def get_privileges(self) -> list[str]:
        """
        Get the list of prileges for the object type
        """
        return self._privileges

    def remove_privilege(self, name: str):
        """
        Remove a privilege from the list
        """
        if name in self._privileges:
            self._privileges.remove(name)

    def plan_privileges(self, action: PrivilegeAction, database_name: str, role_name: str,
                        object_type_scope: str, option: PrivilegeOption) -> str | None:
        """
        Generate the sql statement to grant or revoke privileges
        """
        # check the input values
        if object_type_scope == "all" and self.get_object_type() == "pipes":
            # Bulk grant on objects of type PIPE to DATABASE_ROLE is restricted.
            return None

        # Generate the sql statement to grant or revoke privileges
        sql = "%s %s on %s in database %s %s database role %s.%s %s" % (
            action.value,
            ", ".join(self._privileges),
            f"{object_type_scope} {self.get_object_type()}",
            database_name,
            "to" if action == PrivilegeAction.GRANT else "from",
            database_name,
            role_name,
            option.value)
        return sql
