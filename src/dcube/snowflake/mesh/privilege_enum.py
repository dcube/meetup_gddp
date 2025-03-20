"""..."""
from enum import Enum


class PrivilegeAction(Enum):
    REVOKE = "revoke"
    GRANT = "grant"


class PrivilegeOption(Enum):
    CASCADE = "cascade"
    RESTRICT = "restrict"
    COPY = "copy current grants"
    REVOKE = "revoke current grants"
    EMPTY = ""
