from src.query_parser import QueryParser
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def test_query_parser_simple(simple_query):
    qp = QueryParser(logger, simple_query)
    assert qp.query == simple_query.lower()
    assert qp.query_type == 'SELECT'
    assert qp.source_object == ['db1', 'table_a']


def test_simple_lower_case(lower_case_simple_query):
    qp = QueryParser(logger, lower_case_simple_query)
    assert qp.query == lower_case_simple_query.lower()
    assert qp.query_type == 'SELECT'
    assert qp.source_object == ['db1', 'table_a']


def test_drop_table(drop_table_query):
    qp = QueryParser(logger, drop_table_query)
    assert qp.query == drop_table_query.lower()
    assert qp.query_type == 'DROP'
    assert qp.source_object == ['dev_db', 'table_b']



def test_drop_database(drop_database_query):
    qp = QueryParser(logger, drop_database_query)
    assert qp.query == drop_database_query.lower()
    assert qp.query_type == 'DROP'
    assert qp.source_object == ['sb_production']



def test_remove(Remove_query):
    qp = QueryParser(logger, Remove_query)
    assert qp.query == Remove_query.lower()
    assert qp.query_type == 'REMOVE'
    assert qp.source_object == ['landing_data', 'public', 'attrep_is_landing_data_6af21a79_b97f_7c4f_8513_967ff6a786ce/6af21a79_b97f_7c4f_8513_967ff6a786ce/0/cdc00001296', 'csv']



def test_redacted(redacted_query):
    qp = QueryParser(logger, redacted_query)
    assert qp.query == redacted_query.lower()
    assert qp.query_type == '<REDACTED>'
    assert qp.source_object == []
def test_truncate(truncate_query):
    qp = QueryParser(logger, truncate_query)
    assert qp.query == truncate_query.lower()
    assert qp.query_type == 'TRUNCATE TABLE'
    assert qp.source_object == ['data_loader_status', 'attrep_changesd8492f6541312d7']

def test_alter_session(alter_session_query):
    qp = QueryParser(logger, alter_session_query)
    assert qp.query == alter_session_query.lower()
    assert qp.query_type == 'ALTER SESSION'
    assert qp.source_object == []

def test_create_or_replace(create_or_replace_query):
    qp = QueryParser(logger, create_or_replace_query)
    assert qp.query == create_or_replace_query.lower()
    assert qp.query_type == 'CREATE OR REPLACE'
    assert qp.source_object == ['db2', 'table_b']
