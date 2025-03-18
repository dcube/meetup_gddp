"""..."""
from abc import ABC


class BaseObject(ABC):
    """
    Abstract class for most of the mesh contract objects
    which have name and comment properties
    """
    _name: str
    _comment: str

    def __init__(self, name: str, comment: str) -> None:
        self._name = name
        self._comment = comment

    def get_name(self) -> str:
        """
        Get the name
        """
        return self._name

    def get_comment(self) -> str:
        """
        Get the comment
        """
        return self._comment
