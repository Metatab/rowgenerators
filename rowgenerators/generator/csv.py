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

        assert self.url.scheme == 'file', str(self.url)

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        import csv

        csv.field_size_limit(sys.maxsize) # For: _csv.Error: field larger than field limit (131072)

        self.start()

        with open(self.url.path, encoding=self.url.encoding) as f:
            yield from csv.reader(f, delimiter=self.delimiter)

        self.finish()

