# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
from rowgenerators.exceptions import SourceError, RowGeneratorError
from rowgenerators.source import Source
from rowgenerators.appurl import parse_app_url


def iterate_pandas(df):

    import numpy as np

    if len(df.index.names) == 1 and df.index.names[0] is None and df.index.dtype != np.dtype('O'):
        # For an unnamed, single index, assume that it is just a row number
        # and we don't really need it

        yield list(df.columns)

        for index, row in df.iterrows():
            yield list(row)

    else:

        # Otherwise, either there are more than

        index_names = [n if n else "index{}".format(i) for i, n in enumerate(df.index.names)]

        yield index_names + list(df.columns)

        if len(df.index.names) == 1:
            idx_list = lambda x: [x]
        else:
            idx_list = lambda x: list(x)

        for index, row in df.iterrows():
            yield idx_list(index) + list(row)

def to_codes(df):
    """Return a dataframe with all of the categoricals represented as codes"""
    df = df.copy()
    cat_cols = df.select_dtypes(['category']).columns
    df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
    return df

class StataSource(Source):
    """Generate rows from a stata object """

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        """

        Setting a fragment query with a value of 'values=codes' will
        cause data frame to iterate over codes, rather than categoricals.

        :param ref:
        :param cache:
        :param working_dir:
        :param env:
        :param kwargs:
        """

        super().__init__(ref, cache, working_dir, **kwargs)

        if not working_dir in sys.path:
            sys.path.append(working_dir)

        self.env = env or {}

        source_url = kwargs.get('source_url')

        self.fragment_args = parse_app_url(source_url).fragment_query

        self.value_type = self.fragment_args.get('values', 'categorical')

        self.kwargs = kwargs

    def dataframe(self, *args, **kwargs):
        """Return a pandas dataframe from the resource"""

        import pandas as pd

        return pd.read_stata(self.ref.fspath, *args, **kwargs)

    def __iter__(self):

        df = self.dataframe()

        if self.value_type == 'codes':
            df = to_codes(df)

        yield from iterate_pandas(df)
