"""Class containing methods to interact with Azure Key Vault to retrieve credentials."""

from src.utils import az_secret


def get_azure_credentials(logger, secret_name):
    """Get the credentials from Azure Key Vault."""
    logger.info(f"Getting credentials for {secret_name}")
    credentials = az_secret(secret_name)
    logger.info(f"Credentials for {secret_name} retrieved")
    return credentials
