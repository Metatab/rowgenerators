# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
import os
from rowgenerators.source import Source


class CsvSource(Source):
    """Generate rows from a CSV source"""

    delimiter = ','

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

        if not self.url.exists():
            raise FileNotFoundError(self.url)

        if self.url.scheme != 'file':
            assert self.url.scheme == 'file', str(self.url)

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        import csv

        try:
            # For: _csv.Error: field larger than field limit (131072)
            if os.name == 'nt':
                # Using sys.maxsize throws an Overflow error on Windows 64-bit platforms since internal
                # representation of 'int'/'long' on Win64 is only 32-bit wide. Ideally limit on Win64
                # should not exceed ((2**31)-1) as long as internal representation uses 'int' and/or 'long'
                csv.field_size_limit((2**31)-1)
            else:
                csv.field_size_limit(sys.maxsize) 
        except OverflowError as e:
            # skip setting the limit for now
            pass
        
        self.start()

        try:

            # Python 3.6 considers None to mean 'utf8', but Python 3.5 considers it to be 'ascii'
            encoding = self.url.encoding or 'utf8'

            with open(self.url.fspath, encoding=encoding) as f:
                yield from csv.reader(f, delimiter=self.delimiter)

        except UnicodeError as e:
            raise

        self.finish()

    def dataframe(self, limit=None, *args, **kwargs):
        import pandas

        if self.url.encoding and not 'encoding' in kwargs:
            kwargs['encoding'] = self.url.encoding

        return pandas.read_csv(self.url.fspath, *args, **kwargs)

