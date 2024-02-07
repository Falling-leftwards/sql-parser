"""Module to create the snowflake onnection."""
import json

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def get_snowflake_connection(logger, credentials):
    """Get a connection to Snowflake.

    based on secrtets stored in Azure Key Vault.
    """
    logger.info("Connecting to Snowflake")
    conection_information = json.loads(credentials)
    logger.info("Snowflake connection information loaded")

    snowflake_url = URL(
        account=conection_information["ACCOUNT"],
        user=conection_information["USER"],
        password=conection_information["PASSWORD"],
        database="SNOWFLAKE",
        schema="PUBLIC",
        role=conection_information["ROLE"],
        warehuse=conection_information["WAREHOUSE"],
    )
    engine = create_engine(snowflake_url)
    logger.info("Snowflake connection established")
    snowflake_connection = engine.connect()
    logger.info("Snowflake connection opened")

    return snowflake_connection
