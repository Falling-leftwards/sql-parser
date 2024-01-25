from src.query_parser import QueryParser


def test_query_parser_simple(simple_query):
    qp = QueryParser(simple_query)
    assert qp.query == simple_query
    assert qp.query_type == 'SELECT'
    assert qp.table == 'table_a'
    assert qp.database == 'db1'


def test_simple_lower_case(lower_case_simple_query):
    qp = QueryParser(lower_case_simple_query)
    assert qp.query == lower_case_simple_query
    assert qp.query_type == 'SELECT'
    assert qp.table == 'table_a'
    assert qp.database == 'db1'


def test_drop_table(drop_table_query):
    qp = QueryParser(drop_table_query)
    assert qp.query == drop_table_query
    assert qp.query_type == 'DROP'
    assert qp.table == 'table_b'
    assert qp.database == 'dev_db'


def test_drop_database(drop_database_query):
    qp = QueryParser(drop_database_query)
    assert qp.query == drop_database_query
    assert qp.query_type == 'DROP'
    assert qp.table == ""
    assert qp.database == 'sb_production'
