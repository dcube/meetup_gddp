"""..."""
from dcube.snowflake.mesh.base_object import BaseObject
from dcube.snowflake.mesh.database_role_object_privileges import DatabaseRoleObjectPrivileges as ObjectPrivileges
from dcube.snowflake.mesh.privilege_enum import PrivilegeAction, PrivilegeOption


class DatabaseRole(BaseObject):
    """
    A database role.
    """

    def __init__(self, name: str, comment: str,
                 objects_privileges: list[ObjectPrivileges]) -> None:
        super().__init__(name=name, comment=comment)
        self._objects_privileges = objects_privileges

    def get_objects_privileges(self) -> list[ObjectPrivileges]:
        """
        Get the list of objects' privileges.
        """
        return self._objects_privileges

    def plan_create_or_alter(self, database_name: str) -> str:
        """
        Plan the create or alter statement for the database role.
        """
        sql = "create or alter database role %s.%s comment = '%s'" % (
            database_name, self._name, self._comment.replace("'", "''"))
        return sql

    def plan_grant_to_domain_admin(self, database_name: str,
                                   domain: str) -> str:
        sql = "grant database role %s.%s to role %s_admin" % (
            database_name, self._name, domain)
        return sql

    def plan_objects_privileges(self, action: PrivilegeAction,
                                database_name: str) -> list[str]:
        """
        Generate the sql statements to grant or revoke privileges
        """
        option = PrivilegeOption.EMPTY
        if action == PrivilegeAction.REVOKE:
            option = PrivilegeOption.CASCADE
        elif action == PrivilegeAction.GRANT and self._name == "admin":
            option = PrivilegeOption.COPY

        # Generate the sql statements to grant or revoke privileges
        sqls: list[str] = []
        for object_privileges in self.get_objects_privileges():
            for scope in ["future", "all"]:
                sql = object_privileges.plan_privileges(
                    action=action,
                    database_name=database_name,
                    role_name=self._name,
                    option=option,
                    object_type_scope=scope)
                sqls.append(sql) if sql else None
        return sqls
