import pytest
from src.query_parser import QueryParser

def test_query_parser(simple_query):
    qp = QueryParser(simple_query)
    assert qp.query == simple_query
    assert qp.query_type == 'SELECT'
    assert qp.table == ['TABLE_A']

