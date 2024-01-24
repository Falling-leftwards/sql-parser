import pytest


@pytest.fixture
def simple_query():
    query = """SELECT column_A FROM DB1.TABLE_A"""
    return query
@pytest.fixture
def lower_case_simple_query():
    query = """select column_A from table_A"""
    return query

@pytest.fixture
def drop_table_query():
    query = """DROP TABLE DEV_DB.TABLE_B"""
    return query

@pytest.fixture
def drop_database_query():
    query = """DROP DATABASE SB_PRODUCTION"""
    return query
