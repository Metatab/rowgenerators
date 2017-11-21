# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """



from rowgenerators.exceptions import SchemaError
from tabulate import tabulate

class Table(object):

    def __init__(self, name=None):
        self.name = name
        self.columns = []

    def add_column(self, name, datatype=None, width=None):
        self.columns.append(Column(name, datatype, width))

    @property
    def headers(self):
        return [c.name for c in self.columns]

    def __iter__(self):
        for c in self.columns:
            yield c

    def __str__(self):

        def _dt(dt):
            try:
                return dt.__name__
            except AttributeError:
                return dt

        headers = 'name datatype width'.split()
        rows = [(c.name, _dt(c.datatype), c.width) for c in self.columns]

        return ('Table: {}\n'.format(self.name)) + tabulate(rows, headers)

    def make_fw_row_parser(self, ignore_empty=False):

        parts = []

        start = 0
        for i, c in enumerate(self.columns):

            try:
                int(c.width)
            except TypeError:

                # This is a special name from Metapack, EMPTY_SOURCE_HEADER
                # The value should probably be moved into the rowgenerators code
                if ignore_empty:
                    continue

                raise SchemaError('Table must have width value for {} column '.format(c.name))

            parts.append('row[{}:{}].strip()'.format(start,start + c.width))

            start += c.width

        code = 'lambda row: [{}]'.format(','.join(parts))

        return eval(code)

class Column(object):

    def __init__(self, name, datatype=None, width=None):

        self.name = name
        self.datatype = datatype
        self.width = width


