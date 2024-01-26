import logging

class QueryParser:
    def __init__(self,logger ,query):
        self.logger = logger
        self.query = query.lower()
        self.query_type = None
        self.source_object = []
        self._parse()

    def _parse(self):
        split_query = str.split(self.query)
        self.logger.debug(f"split query {split_query}")

        if split_query[0].upper() in ['ALTER', 'TRUNCATE']:
            statement = split_query[0:2]
            self.query_type = ' '.join(statement).upper()
        elif split_query[0].upper() in ['CREATE']:
            statement = split_query[0:3]
            self.query_type = ' '.join(statement).upper()
        else:
            self.query_type = split_query[0].upper()

        try:
            from_position = split_query.index('from')
        except:
            from_position = None
        self.logger.debug(f"""from positio:{from_position}""")

        if from_position:
            self.logger.debug(f"""source statement:{split_query[from_position + 1]}""")
            source_string = split_query[from_position + 1].replace('"','')
            self.logger.debug(f"""source string:{source_string}""")
            self.source_object = (source_string.split('.'))
            self.logger.debug(f"from table: {self.source_object}")
