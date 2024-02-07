"""Class containing methods to interact with Azure Key Vault to retrieve credentials."""

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient


def get_azure_sf_credentials(logger, kv_url, managed_identity, secret_name):
    """Get snowflake credentials form azure keywault.

    Args: secret_name (str): Name of the secret
    Args: kv_url (str): The url of the key vault
    Args: managed_identity (str): The managed identity to use
    Args: logger (logging.logger): The logger to use

    Returns: str: The secret
    """
    credential = ManagedIdentityCredential(client_id=managed_identity)
    secret_client = SecretClient(vault_url=kv_url, credential=credential)
    logger.info(f"Getting credentials for {secret_name}")
    snowflake_Credentials = secret_client.get_secret(secret_name).value

    logger.info(f"Credentials for {secret_name} retrieved")
    return snowflake_Credentials
