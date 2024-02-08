"""Module contains the function to execute a query against snowflake."""


def perform_snowflake_query(logger, snowflake_conection, query):
    """Used to execute a query against snowflake.

    It contains primary error handling and logging.
    """
    logger.info("Starting Query process")
    try:
        result = snowflake_conection.execute(query).fetchall()
        logger.info("Query executed succsessful")

    except:
        logger.error("Query failed")
        raise Exception("Query failed")

    finally:
        snowflake_conection.close()
        logger.info("Snowflake Connection Closed")

    return result
