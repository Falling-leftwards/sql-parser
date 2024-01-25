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
            if table.db == '' and table.name != '':
                self.table = ''
                self.database = str.lower(table.name)
            elif table.db != '' and table.name != '':
                self.table = str.lower(table.name)
                self.database = str.lower(table.db)
            elif table.db == '' and table.name == '':
                self.table = None
                self.database = None

        split_query = str.split(self.query)

        if split_query[0].upper() in ['ALTER', 'CREATE']:
            self.query_type = split_query[0:1].upper()
        else:
            self.query_type = split_query[0].upper()
#        if parse_one(self.query).find(exp.Select):
#            self.query_type = 'SELECT'
#        elif parse_one(self.query).find(exp.Drop):
#            self.query_type = 'DROP'
