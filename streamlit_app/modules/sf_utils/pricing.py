
"""..."""
from enum import Enum


class Pricing(Enum):
    """ Credit cost by edition, platform, region in $USD """
    STANDARD_AWS_EU_PARIS = 2.6 * 0.95
    ENTERPRISE_AWS_EU_PARIS = 3.9 * 0.95
    BUSINESSCRITICAL_AWS_EU_PARIS = 5.2 * 0.95
    STORAGE_AWS_EU_PARIS = 24 * 0.95

    @classmethod
    def get_credit_price(cls, edition: str, provider_region: str) -> float:
        """
        Get the Snowflake pricing for a specific cloud provider and region.
        :param provider_region: The cloud provider and region in the format "<EDITION>_<PROVIDER>_<REGION>"
        :return: The pricing in USD for the specified provider and region
        :raises ValueError: If the provider_region is not found
        """

        try:
            return cls[f"{edition}_{provider_region}".upper()].value
        except KeyError:
            raise ValueError(f"Pricing for {edition}_{provider_region} not found.")
