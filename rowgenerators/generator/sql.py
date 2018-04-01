# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
from rowgenerators.exceptions import SourceError, RowGeneratorError
from rowgenerators.source import Source

class SqlSource(Source):
    """Generate rows from a callable object. Takes kwargs from the spec to pass into the program. """

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):

        super().__init__(ref, cache, working_dir, **kwargs)

        if not working_dir in sys.path:

            sys.path.append(working_dir)

        self.env = env or {}

        self.kwargs = kwargs

    def __iter__(self):

        from sqlalchemy import create_engine
        from sqlalchemy.exc import  DatabaseError

        try:
            engine = create_engine(self.ref.dsn)
            connection = engine.connect()
        except DatabaseError as e:
            raise RowGeneratorError("Database connection failed for dsn '{}' : {} ".format(self.ref.dsn,str(e)))

        try:
            r = connection.execute(self.ref.sql)
        except DatabaseError as e:
            raise RowGeneratorError("Database query failed for dsn '{}' : {} ".format(self.ref.dsn,str(e) ))

        yield r.keys()

        yield from r

