# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
import os
from rowgenerators.source import Source
import json

class JsonRowSource(Source):
    """Generate rows from a JSON file with an array of rows. """

    delimiter = ','

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

        if not self.url.exists():
            raise FileNotFoundError(self.url)

        if self.url.scheme != 'file':
            assert self.url.scheme == 'file', str(self.url)

        self._meta = {}

    def __iter__(self):
        """Iterate over all of the lines in the file"""


        self.start()

        try:

            # Python 3.6 considers None to mean 'utf8', but Python 3.5 considers it to be 'ascii'
            encoding = self.url.encoding or 'utf8'

            with open(self.url.fspath, encoding=encoding) as f:
                yield from json.load(f)

        except UnicodeError as e:
            raise

        self.finish()

    def finish(self):
        super().finish()


    @property
    def headers(self):
        '''Return the headers. This implementation just returns the first line, which is not always correct'''

        return next(iter(self))


    def dataframe(self, limit=None, *args, **kwargs):
        import pandas

        i = iter(self)

        columns =  next(i)

        rows = list(i)

        return pandas.DataFrame(rows, columns=columns)
