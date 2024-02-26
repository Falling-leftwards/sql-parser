"""Manual CLI for running SnowGuardLogs."""
import logging
import logging.config
import snowflake.connector
import os
from ..src.SnowGuardLogsMonitor import SnowGuardLogMonitor


def SnowfGuard_logs_local():
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger("manual_run")
    logger.info("initate SnowGuard-Logs CLI")
    try:
        SF_USER = os.environ["SF_USER"]
        SF_ACCOUNT = os.environ["SF_ACCOUNT"]
        SF_ROLE = os.environ["SF_ROLE"]
        SF_WAREHOUSE = os.environ["SF_WAREHOUSE"]
        logger.info(f"Snowflake connection info collected from environment variables")
    except:
        logger.info(f"Snowflake connection info collected from user input")
        SF_USER = input("Enter Snowflake user: ")
        SF_ACCOUNT = input("Enter Snowflake account: ")
        SF_ROLE = input("Enter Snowflake role: ")
        SF_WAREHOUSE = input("Enter Snowflake warehouse: ")

    logger.info(f"Snowflake connection info collected")
    try:
        snowflake_connection = snowflake.connector.connect(
            user=SF_USER,
            authenticator="externalbrowser",
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database="DDS_CORE_DATA_PLATFORM",
            schema="SNOWFLAKE_MONITORING",
            role=SF_ROLE,
        )
    except:
        logger.exception(f"Snowflake connection failed")
        raise Exception("Snowflake connection failed")

    SnowGuardLogMonitor(snowflake_connection, logger)


if __name__ == "__main__":
    SnowfGuard_logs_local()
    os.remove("logs/monitor.log")
