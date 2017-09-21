# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
from rowgenerators.source import Source


class CsvSource(Source):
    """Generate rows from a CSV source"""

    delimiter = ','

    def __init__(self, ref, cache=None, working_dir=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

        if not self.url.exists():
            raise FileNotFoundError(self.url)

        if self.url.scheme != 'file':
            assert self.url.scheme == 'file', str(self.url)

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        import csv

        csv.field_size_limit(sys.maxsize) # For: _csv.Error: field larger than field limit (131072)

        self.start()

        try:

            # Python 3.6 considers None to mean 'utf8', but Python 3.5 considers it to be 'ascii'
            encoding = self.url.encoding or 'utf8'

            with open(self.url.path, encoding=encoding) as f:
                yield from csv.reader(f, delimiter=self.delimiter)
        except UnicodeError as e:
            raise

        self.finish()