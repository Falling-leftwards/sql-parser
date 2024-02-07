"""Tests for the rules engine."""
import logging


from src.RulesEngine import RulesEngine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def test_simple_rule():
    """test to check if the rules engine is working."""
    rule = [{"type": "exception", "who": "USER", "where": "DB1", "what": "DROP"}]
    who = "USER"
    where = ["DB1"]
    what = "DROP"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def test_case_diff():
    """test is to check if the rules engine is case insensitive."""
    rule = [{"type": "exception", "who": "USER", "where": "DB1", "what": "DROP"}]
    who = "user"
    where = ["db1"]
    what = "drop"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def test_notification_simple():
    """test to check if the rules engine is working."""
    rule = [{"type": "exception", "who": "USER", "where": "DB1", "what": "DROP"}]
    who = "user"
    where = ["db1"]
    what = "create"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is True


def test_regex_who():
    """Test to check if the rules engine is working using regex."""
    rule = [
        {
            "type": "exception",
            "who": ".*@testcomp.dum",
            "where": "DB1",
            "what": "CREATE",
        }
    ]
    who = "user@testcomp.dum"
    where = ["db1"]
    what = "CREATE"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def test_multi_location():
    """Test to check if the rules engine is working with multiple locations."""
    rule = [
        {
            "type": "exception",
            "who": "user@testcomp.dum",
            "where": "SNAPSHOT",
            "what": "DROP",
        }
    ]
    who = "user@testcomp.dum"
    where = ["db1", "SNAPSHOT", "random_table"]
    what = "drop"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def teat_empty_location():
    """Test to check if the rules engine is working with empty locations."""
    rule = [{"type": "exception", "who": ".*", "where": ".*", "what": ".*"}]
    who = "USER"
    where = []
    what = "CREATE"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def test_put_rule():
    """Test to check if the rules engine is working with PUT.

    As PUT has caused problems in the past."""
    rule = [{"type": "exception", "who": ".*", "what": "PUT", "where": ".*"}]
    who = "USER"
    where = ["DB1"]
    what = "PUT"

    rulestest = RulesEngine(logger, rule, who, where, what)
    assert rulestest.notification is False


def test_multiple_rules():
    """Test to check if the rules engine is working with multiple rules."""
    rule = [
        {
            "type": "exception",
            "who": "SYS_TERRAFORM_PLATFORM",
            "what": "CREATE",
            "where": "DEV_.*",
        },
        {
            "type": "exception",
            "who": "SYS_TERRAFORM_PLATFORM",
            "what": "DROP",
            "where": "DEV_.*",
        },
    ]
    assert (
        RulesEngine(
            logger, rule, "SYS_TERRAFORM_PLATFORM", ["DEV_DB1"], "CREATE"
        ).notification
        is False
    )
    assert (
        RulesEngine(
            logger, rule, "SYS_TERRAFORM_PLATFORM", ["DEV_DB1"], "DROP"
        ).notification
        is False
    )
    assert (
        RulesEngine(
            logger, rule, "SYS_TERRAFORM_PLATFORM", ["DEV_DB2"], "CREATE"
        ).notification
        is False
    )
    assert (
        RulesEngine(
            logger, rule, "SYS_TERRAFORM_PLATFORM", ["DEV_DB2"], "DROP"
        ).notification
        is False
    )
    assert (
        RulesEngine(
            logger, rule, "SYS_TERRAFORM_PLATFORM", ["DB1"], "CREATE"
        ).notification
        is True
    )
