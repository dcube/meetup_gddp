"""..."""
from dcube.snowflake.mesh.base_object import BaseObject

class Domain(BaseObject):
    """
    A data domain
    """
    def __init__(self, name: str, comment: str) -> None:
        super().__init__(name=name, comment=comment)
