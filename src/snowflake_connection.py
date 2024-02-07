"""Module to create the snowflake onnection."""
import json

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from src.az_utils import az_secret


def get_snowflake_connection(logger, snowflake_user, snowflake_role):
    """Get a connection to Snowflake.

    based on secrtets stored in Azure Key Vault.
    """
    logger.info("Connecting to Snowflake")
    credentials = az_secret(snowflake_user)
    conection_information = json.loads(credentials)
    logger.info("Snowflake connection information loaded")

    snowflake_url = URL(
        account=conection_information["ACCOUNT"],
        user=conection_information["USER"],
        password=conection_information["PASSWORD"],
        database="SNOWFLAKE",
        schema="PUBLIC",
        role=snowflake_role,
        warehuse="TRANSFORMING_PLATFORM_XS",
    )
    engine = create_engine(snowflake_url)
    logger.info("Snowflake connection established")
    snowflake_connection = engine.connect()
    logger.info("Snowflake connection opened")

    return snowflake_connection
