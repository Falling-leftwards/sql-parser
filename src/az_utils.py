"""Utility functions for Azure Key Vault and other Azure servicesi"""
import json
import re

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from prefect.blocks.system import String


def az_secret(secret_name):
    """Get a secret from Azure Key Vault.

    Args: secret_name (str): Name of the secret
    Returns: str: The secret
    """
    kv_url = String.load("kv-name").value
    mi_id = String.load("akspool-mi").value

    credential = ManagedIdentityCredential(client_id=mi_id)
    secret_client = SecretClient(vault_url=kv_url, credential=credential)
    secret_value = secret_client.get_secret(secret_name).value
    return secret_value


def slugify(string):
    """Slugify a string.

    Args: string (str): String to slugify
    Returns: str: Slugified string.
    """
    return re.sub(r"[^a-z0-9]", "-", string.lower())


def get_exception_rules():
    """Get the exception rules from the json file."""
    with open("exceptions.json", "r") as j:
        exception_rules = json.loads(j.read())
    return exception_rules
