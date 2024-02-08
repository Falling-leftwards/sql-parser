"""Flow is used to monitor the Snowflake Query History."""
import sys

from prefect import flow, get_run_logger, settings
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.context import get_run_context

from src.az_utils import get_exception_rules
from src.query_parser import QueryParser
from src.RulesEngine import RulesEngine
from src.snowflake_connection import get_snowflake_connection
from src.snowflake_query import perform_snowflake_query


@flow(name="SnowflakeMonitor")
def SnowflakeMonitor():
    """Flow is used to monitor the Snowflake Query History.

    It will get the last 6 hours of query history and analyse the queries.
    it will compare the queries to the exceptions found in exception.json.
    If the query does not match any of the exceptions
    it will send a notification to the teams channel.
    """
    logger = get_run_logger()
    # Snowflake connection
    try:
        logger.info("Getting Snowflake connection")
        snowflake_connection = get_snowflake_connection(
            logger, "prefect-janitor", "DATA_ENGINEER_PLATFORM"
        )
    except:
        logger.info("Failed to get snowflake connection")
        raise Exception("Snowflake connection failed")

    # Query
    query = """
    SELECT
    START_TIME,
    END_TIME,
    USER_NAME,
    EXECUTION_STATUS,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TEXT,
    QUERY_TAG ,
    QUERY_TYPE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE (QUERY_TYPE NOT IN (
    'EXECUTE_STREAMLIT',
    'GET_FILES',
    'SELECT',
    'SHOW',
    'DESCRIBE'
    ))
    AND (EXECUTION_STATUS = 'SUCCESS')
    AND (START_TIME >= DATEADD(HOUR, -6, CURRENT_TIMESTAMP()))
ORDER BY START_TIME DESC
    """

    query_result = perform_snowflake_query(logger, snowflake_connection, query)
    logger.info("Query executed succsessful")
    logger.info("getting Whitelist")
    exeption_rules = get_exception_rules()
    logger.debug(f"Whitelist: {exeption_rules}")
    # anysis
    rules_breached = []
    logger.info(f"Analysing {len(query_result)} queries")
    for snowflake_query in query_result:
        logger.debug(f"Snowflake query: {snowflake_query}")
        logger.debug(f"Snowflake query text: {snowflake_query[6]}")
        qp = QueryParser(logger, snowflake_query[6])
        query_type = qp.query_type
        query_object = qp.get_query_object()
        logger.debug(f"Query type: {query_type}")
        logger.debug(f"Query object: {query_object}")
        query_creator = snowflake_query[2]
        logger.debug(f"Query creator: {query_creator}")
        if (len(query_object) < 3) & (snowflake_query[4] is not None):
            query_object += [snowflake_query[4]]
        else:
            query_object = query_object

        analysis_result = RulesEngine(
            logger, exeption_rules, query_creator, query_object, query_type
        )
        logger.debug(f"Analysis result: {analysis_result.notification}")

        if analysis_result.notification is True:
            rules_breached.append(
                f"""Start time: {snowflake_query[0]} Who: {query_creator} What: {query_type} Where: {query_object} Query text: {snowflake_query[6]}
                                   """
            )
    logger.info("Query analysis completed")
    logger.info(f"Queries that breached rules: {len(rules_breached)}")
    if len(rules_breached) > 0:
        notification_string = f"""Number of breaches: {len(rules_breached)}\n
        Queries that breached the rules:\n
        """
        logger.info("creating notification string")
        for query in rules_breached:
            notification_string += f"{query}\n"
            logger.warn(f"Breached query: {query}")
    else:
        notification_string = ""

    notification_size = sys.getsizeof(notification_string)
    logger.info(
        f"""The size of the notification sting in memmory:
        {sys.getsizeof(notification_size)} Bytes"""
    )
    # notification
    # send notifications to teams channel
    flow_run = get_run_context().flow_run
    flow_run_url = f"{settings.PREFECT_UI_URL.value()}/flow-runs/flow-run/{flow_run.id}"

    teams_webhook_block = MicrosoftTeamsWebhook.load("log-monitoring")
    if notification_string not in ["", None]:
        logger.info("sending notification to teams")
        error_header = "Logg monitor detected breaches\n\n"
        if notification_size > 1024:
            logger.info("The notification size is grear than 1KB")
            teams_webhook_block.notify(
                body=f"""{error_header}\nTotal number of
                                       breeches is:{len(rules_breached)}\n
                                       Notification size to large to send\n
                                      go to the logs for more information\n
                                       Flow run url: {flow_run_url}"""
            )
        else:
            teams_webhook_block.notify(body=f"{error_header}\n{notification_string}")

    else:
        logger.info("No error detected")


if __name__ == "__main__":
    SnowflakeMonitor()
