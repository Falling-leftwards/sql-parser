"""Main function containg the runtime logic that is independent of thedeployment environment.
"""

from src.SnowflakeConnection import get_snowflake_connection
from src.SnowflakeQuery import perform_snowflake_query
from src.QueryParser import QueryParser
from src.RulesEngine import RulesEngine


class SnowGuardLogs:
    """System logic for gathering and analysing the snowflag query history.

    This function contains the core logic of the system
    that is independent of the deployment mechanism.
    It will need to be called with arguments that provide the neccecary
    credentials, as well as pathway for notification.
    """

    def __init__(self, logger, snowflake_credentials, query_whiteList):
        """Initialise the SnowGuardLogs object."""
        self.logger = logger
        self.snowflake_credentials = snowflake_connection
        self.query_whiteList = query_whiteList
        self.logger.info("SnowGuardLogs object initialised")
        self.main()

    def main(self):
        """Main function for the SnowGuardLogs object."""
        self.logger.info("Getting Snowflake connection")
        try:
            snowflake_connection = get_snowflake_connection(
                self.logger, self.snowflake_credentials
            )
            self.logger.info("Snowflake connection successful")
        except Exception as e:
            self.logger.error("Failed to get snowflake connection")
            raise e("Snowflake connection failed, Check provided credentials ")

        snowflake_account_usage_query = """
        SELECT
        START_TIME,
        USER_NAME,
        EXECUTION_STATUS,
        DATABASE_NAME,
        QUERY_TEXT,
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

        try:
            query_result = perform_snowflake_query(
                self.logger, snowflake_connection, snowflake_account_usage_query
            )
            self.logger.info("Query executed succsessful")
        except Exception as e:
            self.logger.error("Failed to get query result")
            query_result = []
            raise e("Query failed after connection was established")

        self.rules_breached = []
        query_counter = 0
        self.logger.info(f"Analysing {len(query_result)} queries")
        for snowflake_query in query_result:
            parsed_query = None
            query_type = None
            query_object = None
            query_creator = None
            query_compliance = None
            breach_log_entry = None
            self.logger.debug(f"Resetting variables for query {query_counter}")
            self.logger.debug(f"Snowflake query: {snowflake_query}")
            self.logger.debug(f"Snowflake query text: {snowflake_query[4]}")
            parsed_query = QueryParser(self.logger, snowflake_query[4])

            query_type = parsed_query.get_query_type()
            query_object = parsed_query.get_query_object()
            self.logger.debug(f"Query type: {query_type}")
            self.logger.debug(f"Query object: {query_object}")

            query_creator = snowflake_query[1]
            self.logger.debug(f"Query creator: {query_creator}")

            query_counter += 1
            self.logger.debug(f"{query_counter} queries parsed")

            if (len(query_object) < 3) & (snowflake_query[3] is not None):
                query_object += [snowflake_query[3]]
                self.logger.debug(f"Query object updated: {query_object}")
            else:
                query_object = query_object

            self.logger.debug(f"Assessing rules compliance for query {query_counter}")
            query_compliance = RulesEngine(
                self.logger,
                self.query_whiteList,
                query_creator,
                query_object,
                query_type,
            )
            self.logger.debug(f"Analysis result: {query_compliance.notification}")
            if query_compliance.notification is True:
                breach_log_entry = f"""Start Time: {snowflake_query[0]}, Who {query_creator}, What: {query_type}, Where: {query_object}, Query text: {snowflake_query[4]}"""

                self.rules_breached.append(breach_log_entry)
                self.logger.warn(f"Rule breached: {breach_log_entry}")
        self.logger.info("Query analysis completed")
        self.logger.info(f"Queries that breached rules: {len(self.rules_breached)}")
        return self.rules_breached
