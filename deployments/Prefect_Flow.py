"""Prefect flow for running SnowGuard Logs Monitor with Teams Notification"""
from prefect import flow, get_run_logger, settings
from ..src.SnowGuardLogsMonitor import SnowGuardLogsMonitor
from ..src.teamsNotification import PrefectTeamsNotification
from ..src.azureKeyVault import get_secret

@flow(name="SnowGuardLogsMonitor")
def snow_guard_logs_monitor():
    """Flow to run SnowGuardLogsMonitor"""
    logger = get_run_logger()
    logger.info("SnowGuardLogsMonitor flow started")
    
    logger.info("Getting snowflake credentials from Azure Key Vault")
    snowflake_credentials = get_secret("snowflake-credentials")
    logger.info("Snowflake credentials acquired")
    
    logger.info("Getting query white list from ")
    query_whiteList = get_secret("query-white-list")
    logger.info("Query white list acquired")
    
    logger.info("Initialising SnowGuardLogsMonitor object")
    snow_guard_logs_monitor = SnowGuardLogsMonitor(logger, snowflake_credentials, query_whiteList)
    logger.info("Starting SnowGuardLogsMonitor")
    notification_log = snow_guard_logs_monitor.run()
    logger.info("SnowGuardLogsMonitor finished")

    logger.info("Initialising PrefectTeamsNotification object")
    teams_notification = PrefectTeamsNotification(logger, "teams_webhook", notification_log)
    logger.info("Sending notification to teams")
    teams_notification.send_notification()
    logger.info("Notification sent to teams")
    logger.info("SnowGuardLogsMonitor flow finished")

if __name__ == "__main__":
    """Run the SnowGuardLogsMonitor flow"""
    snow_guard_logs_monitor.run()
