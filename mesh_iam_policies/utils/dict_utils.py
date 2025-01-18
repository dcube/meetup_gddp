"""..."""
from typing import Any


class DictUtils():
    """
    The class DictUtils has a set of static methods to validate
    the structure of a dictionnary
    """

    @staticmethod
    def check_required_keys(dict_to_validate: dict[Any, Any],
                            required_keys: dict[str, Any]) -> None:
        """
        Check that each required key is present and has the correct type
        Args:
            - dict_to_validate: the dictionnary to check
            - required_keys: the dictionary that define the required keys
            and their types
        """
        # loop over the required keys
        for key, value in required_keys.items():
            # check if the key is not missing
            if key not in dict_to_validate:
                raise ValueError(f"Missing key: {key}")
            # check if the type value is correct
            if not isinstance(dict_to_validate[key], value["type"]):
                raise TypeError(
                    "Incorrect type for key: %s got: %s expected: %s" %
                    (key, type(dict_to_validate[key]), str(value["type"])))
            # check if the value is correct
            if "accepted_values" in value and dict_to_validate[
                    key] not in value["accepted_values"]:
                raise ValueError(f"Invalid value for key: {key}")
