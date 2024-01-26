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


def test_remove(Remove_query):
    qp = QueryParser(Remove_query)
    assert qp.query == Remove_query
    assert qp.query_type == 'REMOVE'
    assert qp.table == ""
    assert qp.database == "landing_data"


def test_redacted(redacted_query):
    qp = QueryParser(redacted_query)
    assert qp.query == redacted_query
    assert qp.query_type == 'REDACTED'
    assert qp.table == None
    assert qp.database == None

def test_truncate(truncate_query):
    qp = QueryParser(truncate_query)
    assert qp.query == truncate_query
    assert qp.query_type == 'TRUNCATE'
    assert qp.table == "attrep_changesd8492f6541312d7"
    assert qp.database == "data_loader_status"


def test_alter_session(alter_session_query):
    qp = QueryParser(alter_session_query)
    assert qp.query == alter_session_query
    assert qp.query_type == 'ALTER SESSION'
    assert qp.table == None
    assert qp.database == None


def test_create_or_replace(create_or_replace_query):
    qp = QueryParser(create_or_replace_query)
    assert qp.query == create_or_replace_query
    assert qp.query_type == 'CREATE OR REPLACE'
    assert qp.table == "table_b"
    assert qp.database == "db2"
