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

        if split_query[0].upper() in ['ALTER', 'TRUNCATE', 'BEGIN', 'EXECUTE',
                                      'DROP']:
            statement = split_query[0:2]
            self.query_type = ' '.join(statement).upper().replace(';', '')
        elif split_query[0].upper() in ['CREATE']:
            if split_query[1].upper() in ['USER', 'ROLE', 'TASK', 'TABLE', 'TAG']:
                statement = split_query[0:2]
            else:
                statement = split_query[0:3]
            self.query_type = ' '.join(statement).upper()
        else:
            self.query_type = split_query[0].upper()

        try:
            from_position = split_query.index('from')
        except:
            from_position = None
        self.logger.debug(f"""from position:{from_position}""")

        if from_position:
            self.logger.debug(f"""source statement:{split_query[from_position + 1]}""")
            source_string = split_query[from_position + 1].replace('"', '')
            self.logger.debug(f"""source string:{source_string}""")
            self.source_object = (source_string.split('.'))
            self.logger.debug(f"source object: {self.source_object}")

        if split_query[0].upper() in ['REVOKE', 'GRANT']:
            on_position = split_query.index('on')
            self.logger.debug(f"""on position:{on_position}""")
            if split_query[on_position + 1].upper() in ['FUTURE']:
                in_position = split_query.index('in')
                source_string = split_query[in_position + 2]
                self.logger.debug(f"""source string:{source_string}""")

            else:
                source_string = split_query[on_position + 2].replace('"', '')
                self.logger.debug(f"""source string:{source_string}""")
            self.source_object = (source_string.split('.'))

        if split_query[0].upper() in ['CREATE']:
            if split_query[2].upper() in ['REPLACE']:
                as_position = split_query.index('as')
                self.logger.debug(f"""as position:{as_position}""")
                source_string = split_query[as_position - 1].replace('"', '')
                self.logger.debug(f"""source string:{source_string}""")
                self.source_object = source_string.split('.')
            elif split_query[1].upper() in ['USER', 'ROLE']:
                self.source_object = [split_query[2]
                             .replace('"', '')
                             .replace("'", '')
                             .replace(';' ,'')]
                self.logger.debug(f"""source object:{self.source_object}""")
            elif split_query[1].upper() in ['TASK', 'TABLE', 'TAG']:
                source_string = (split_query[2]
                             .replace('"', '')
                             .replace("'", '')
                             .replace(';' ,''))
                self.logger.debug(f"""source string:{source_string}""")
                self.source_object = source_string.split('.')
                self.logger.debug(f"""source object:{self.source_object}""")

        if split_query[0].upper() in ['SET']:
            self.source_object = [split_query[1]]

        elif split_query[0].upper() in ['TRUNCATE', 'UNDROP', 'MERGE',
                                        'INSERT']:
            source_string = (split_query[2]
                             .replace('"', '')
                             .replace("'", '')
                             .replace(';' ,'')
                             )
            self.source_object = (source_string.split('.'))

        elif split_query[0].upper() in ['DROP','ALTER', 'EXECUTE', 'DESCRIBE']:
            if split_query[1].upper() in ['SESSION']:
                self.source_object = []
            elif split_query[1].upper() in ['TABLE', 'VIEW', 'SCHEMA','USER',
                                            'TASK']:
                if split_query[2].find('identifier') != 1:
                    self.logger.debug(f'identifier found in {split_query[2]}')
                    source_string = (split_query[2]
                                     .replace('identifier', '')
                                     .replace('(', '')
                                     .replace(')', '')
                                     .replace('"', '')
                                     .replace("'", '')
                                     .replace(';', '')
                                    )
                    self.logger.debug(f"""source string:{source_string}""")
                    self.source_object = (source_string.split('.'))
                else:
                    source_string = (split_query[2]
                                     .replace('"', '')
                                     .replace("'", '')
                                     .replace('(', '')
                                     .replace(')', '')
                                     )

                    self.source_object = (source_string.split('.'))
            elif split_query[1].upper() in ['NETWORK', 'MASKING']:
                source_string = (split_query[3]
                                     .replace('"', '')
                                     .replace("'", '')
                                     .replace('(', '')
                                     .replace(')', '')
                                     .replace(';', '')
                                     )
                self.source_object = (source_string.split('.'))
            else:
                source_string = (split_query[2]
                                     .replace('"', '')
                                     .replace("'", '')
                                     .replace('(', '')
                                     .replace(')', '')
                                     )
                self.source_object = (source_string.split('.'))

        elif split_query[0].upper() in ['REMOVE']: 
            source_string = (split_query[1]
                             .replace('"', '')
                             .replace("'", '')
                             .replace('@', '')
                             .replace(';', '')
                             )
            self.source_object = (source_string.split('.'))
        elif split_query[0].upper() in ['LIST']:
            source_string = (split_query[1]
                             .replace('"', '')
                             .replace("'", '')
                             .replace('@', '')
                             .replace(';', '')
                             )
            self.source_object = (source_string.split('.'))

        
        self.logger.debug(f"source object: {self.source_object}")
