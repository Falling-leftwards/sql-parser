import logging


def stringcleanup(string):
    string = string.replace('"', "")
    string = string.replace("'", "")
    string = string.replace(";", "")
    string = string.replace("(", "")
    string = string.replace(")", "")
    string = string.replace("!", "")
    string = string.replace("@", "")
    string = string.replace("IDENTIFIER", "")
    return string


def locate_source(split_query, seperator=".", logger=logging.getLogger()):
    first_dot_found = False
    string_segment = None
    for segment in split_query:
        if (segment.find(seperator) != -1) & (first_dot_found is False):
            first_dot_found = True
            logger.debug(f"""dot found in {segment}""")
            string_segment = stringcleanup(segment)
    return string_segment


class QueryParser:
    def __init__(self, logger, query):
        self.logger = logger
        self.query = query.upper()
        self.query_type = None
        self.source_object = []
        self.source_string = None
        self._parse()

    def _parse(self):
        split_query = str.split(self.query)
        self.logger.debug(f"split query {split_query}")
        statement = None

        # Gather the Query type statement elements
        if split_query[0] in ["BEGIN"]:
            statement = split_query[0:2]

        elif split_query[0] in ["ALTER", "TRUNCATE", "EXECUTE", "DROP"]:
            if split_query[1] in ["ROW"]:
                statement = split_query[0:3]

            else:
                statement = split_query[0:2]

        elif split_query[0] in ["CREATE"]:
            if split_query[1] in [
                "USER",
                "ROLE",
                "TASK",
                "TABLE",
                "TAG",
                "SCHEMA",
            ]:
                statement = split_query[0:2]
            else:
                statement = split_query[0:3]

        if statement:
            self.query_type = stringcleanup(" ".join(statement))
        else:
            self.query_type = split_query[0]



        if split_query[0] in ["REVOKE", "GRANT", 'CREATE']:
            self.source_string =locate_source(split_query, seperator="_") 
            self.logger.debug(f"""source string:{self.source_string}""")



        elif split_query[0] in ["SET"]:
            self.source_string = split_query[1]

        elif split_query[0] in [
            "TRUNCATE",
            "UNDROP",
            "MERGE",
            "INSERT",
            "COPY",
        ]:
            self.source_string = stringcleanup(split_query[2].split("(")[0])

        elif split_query[0] in ["DROP", "ALTER", "EXECUTE", "DESCRIBE", "LIST"]:
            if split_query[1] in ["SESSION"]:
                self.source_object = []

            else:
                self.source_string = locate_source(split_query, seperator="_")

        elif split_query[0] in ["REMOVE", "CALL"]:
            self.source_string = stringcleanup(split_query[1].split("!")[0])
        elif split_query[0] in ["PUT", 'BEGIN', 'GET', ]:
            self.source_string = []

        '''
        Attempt to locate source_string based on the location of from statement
        '''
        try:
            from_position = split_query.index("FROM")
        except:
            from_position = None
        self.logger.debug(f"""from position:{from_position}""")

        if from_position:
            self.source_string = locate_source(split_query)
            self.logger.debug(f"""source string:{self.source_string}""")

        '''
        If source_string not found by now, attempt to find source string
        based on precence of .
        '''

        if self.source_string is None:
            try:
                self.source_string = locate_source(split_query)
            except:
                self.source_string = None

        '''Transfer the identified source string to source object for return'''
        try:
            self.source_object = self.source_string.lower().split('.')
        except:
            if self.source_string is None:
                self.source_object = []
            elif self.source_string == []:
                self.source_object = []
            else:
                self.source_object = [self.source_string.lower()]
        self.logger.debug(f"source object: {self.source_object}")
