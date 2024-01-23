import pytest


@pytest.fixture
def simple_query():
    query = """SELECT column_A FROM TABLE_A"""
    return query
@pytest.fixture
def lower_case_simple_query():
    query = """select column_A from table_A"""
    return query


