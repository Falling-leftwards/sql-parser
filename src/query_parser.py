class QueryParser:
    def __init__(self, query):
        self.query = query
        self.query_type = None
        self.table = None
        self._parse()

    def _parse(self):
        query_type, table = self.query.split()
        self.query_type = query_type
        self.table = [table]
