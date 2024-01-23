import sqlparse

class QueryParser:
    def __init__(self, query):
        self.query = query
        self.query_type = None
        self.table = None
        self._parse()

    def _parse(self):
        query_type = sqlparse.parse(self.query)
        parsed  = sqlparse.parse(self.query)
        self.query_type = query_type[0].get_type()
        print([str(t) for t in parsed[0].tokens if t.ttype is None][0])
        print([t.ttype for t in parsed[0].tokens])
