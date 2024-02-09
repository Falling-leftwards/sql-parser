"""A rules engine to analyse queries and determine notification status."""
import re


class RulesEngine:
    """Class is used to analyse a query against a set of rules.

    The essential concept of the exception is to define who, what and where.
    And then compare the query to the exception.
    If the query matches the exception it will not be reported.
    a query need only match one exception to be excluded from the report.
    """

    def __init__(self, logger, rules, who, where, what):
        """Inintiate the rules engine."""
        self.rules = rules
        self.who = who.lower()
        self.where = [obj.lower() for obj in where]
        self.what = what.lower()
        self.notification = True
        self.logger = logger
        self.analyse()

    def analyse(self):
        """Analyse queries and determin notification status.

        and contains the logic to compare the query to the exception.
        The function will return a boolean value.
        True if the query does not match any of the exceptions.
        """
        for rule in self.rules:
            match_who = False
            match_what = False
            match_where = False
            self.logger.debug("rule test initated")

            if re.search(rule["who"].lower(), self.who):
                match_who = True

            if re.search(rule["what"].lower(), self.what):
                match_what = True

            for location in self.where:
                if re.search(rule["where"].lower(), location):
                    match_where = True

            if (match_who == True) & (match_where == True) & (match_what == True):
                self.notification = False

            self.logger.debug(f"rule: {rule}")
            self.logger.debug(f"query who: {self.who}")
            self.logger.debug(f"query what: {self.what}")
            self.logger.debug(f"query where: {self.where}")
            self.logger.debug(f"state match who {match_who}")
            self.logger.debug(f"state match what {match_what}")
            self.logger.debug(f"state match where {match_where}")
        return self.notification
