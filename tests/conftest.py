import pytest


@pytest.fixture
def simple_query():
    query = """SELECT column_A FROM DB1.TABLE_A"""
    return query


@pytest.fixture
def lower_case_simple_query():
    query = """select column_A from db1.table_A"""
    return query


@pytest.fixture
def drop_table_query():
    query = """DROP TABLE DEV_DB.TABLE_B"""
    return query


@pytest.fixture
def drop_database_query():
    query = """DROP DATABASE SB_PRODUCTION"""
    return query


@pytest.fixture
def Remove_query():
    query = """REMOVE
    @'LANDING_DATA'.'PUBLIC'.'ATTREP_IS_LANDING_DATA_6af21a79_b97f_7c4f_8513_967ff6a786ce'/6af21a79_b97f_7c4f_8513_967ff6a786ce/0/CDC00001296.csv'"""
    return query


@pytest.fixture
def redacted_query():
    query = """<redacted>"""
    return query


@pytest.fixture
def truncate_query():
    query = """TRUNCATE TABLE 'DATA_LOADER_STATUS'.'attrep_changesD8492F6541312D7'"""
    return query


@pytest.fixture
def alter_session_query():
    query = """ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"""
    return query


@pytest.fixture
def create_or_replace_query():
    query = """CREATE OR REPLACE TABLE "DB1"."TABLE_A" AS SELECT * FROM "DB2"."TABLE_B" """
    return query
