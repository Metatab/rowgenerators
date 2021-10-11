# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
import os
from rowgenerators.source import Source
import warnings

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

        self._meta = {}

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

    def finish(self):
        super().finish()


    @property
    def headers(self):
        '''Return the headers. This implementation just returns the first line, which is not always correct'''

        return next(iter(self))


    def dataframe(self, limit=None, *args, **kwargs):
        import pandas
        from rowgenerators.exceptions import RowGeneratorError, RowGeneratorConfigError

        # The NA Filter can produce unfortunate results when it isn't expected.
        # It can also break things when it is turned off
        # if not 'na_filter' in kwargs:
        #    kwargs['na_filter'] = False

        if self.url.encoding and not 'encoding' in kwargs:
            kwargs['encoding'] = self.url.encoding

        last_exception = None

        while True:

            try:
                return pandas.read_csv(self.url.fspath, *args, **kwargs)
            except Exception as e:
                last_exception = e

            if 'not in list' in str(last_exception) and 'parse_dates' in kwargs:

                # This case happens when there is a mismatch between the headings in the
                # file we're reading and the schema. for insance, the file header has a leading space,
                # and we're trying to parse dates for that column. So, try again
                # without parsing dates.
                #del kwargs['parse_dates']
                #warnings.warn('Date parsing error in read_csv. Trying again without parsing dates '+
                #              'Exception: '+str(last_exception))

                raise RowGeneratorConfigError('dates',
                                              'Date parsing error in read_csv. Trying again without parsing dates. '+
                                              'Exception: '+str(last_exception))

            if 'has NA values in column' in str(last_exception) and 'dtype' in kwargs:
                # We're trying to force dtypes to in for a column that can't be an int,
                # so give up trying to force dtypes.

                #del kwargs['dtype']
                #warnings.warn('Error setting dtypes; NA in integer column. Try again without dtypes ' +
                #              'Exception: ' + str(last_exception))
                raise RowGeneratorConfigError('dtype',
                                              'Error setting dtypes; NA in integer column. Try again without dtypes. ' +
                                              'Exception: ' + str(last_exception))

            break


        raise RowGeneratorError(f"{last_exception} in read_csv: path={self.url.fspath} args={args} kwargs={kwargs}\n")


