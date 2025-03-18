"""..."""
from dcube.snowflake.mesh.base_object import BaseObject

class DomainMultiple(BaseObject):
    """
    A domain multiple, it can be by env, layer,
    data products type, a combination of anything else
    """
    def __init__(self, name: str, comment: str) -> None:
        super().__init__(name=name, comment=comment)
