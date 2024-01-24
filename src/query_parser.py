from sqlglot import parse_one, exp


class QueryParser:
    def __init__(self, query):
        self.query = query
        self.query_type = None
        self.database = None
        self.table = None
        self._parse()

    def _parse(self):
        for table in parse_one(self.query).find_all(exp.Table):
            self.table = table.name
            self.database = table.db
        if parse_one(self.query).find(exp.Select):
            self.query_type = 'SELECT'
        elif parse_one(self.query).find(exp.Drop):
            self.query_type = 'DROP'
