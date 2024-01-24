from src.query_parser import QueryParser


def test_query_parser_simple(simple_query):
    qp = QueryParser(simple_query)
    assert qp.query == simple_query
    assert qp.query_type == 'SELECT'
    assert qp.table == 'TABLE_A'
    assert qp.database == 'DB1'


def test_simple_lower_case(lower_case_simple_query):
    qp = QueryParser(lower_case_simple_query)
    assert qp.query == lower_case_simple_query
    assert qp.query_type == 'SELECT'
    assert qp.table == 'table_A'
    assert qp.database == ""


def test_drop_table(drop_table_query):
    qp = QueryParser(drop_table_query)
    assert qp.query == drop_table_query
    assert qp.query_type == 'DROP'
    assert qp.table == 'TABLE_B'
    assert qp.database == 'DEV_DB'


def test_drop_database(drop_database_query):
    qp = QueryParser(drop_database_query)
    assert qp.query == drop_database_query
    assert qp.query_type == 'DROP'
    assert qp.table == ""
    assert qp.database == 'SB_PRODUCTION'
