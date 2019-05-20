# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
from rowgenerators.exceptions import SourceError, RowGeneratorError
from rowgenerators.source import Source
from rowgenerators.appurl import parse_app_url
import pandas as pd
import numpy as np


def iterate_pandas(df):


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

def make_cat_map(df, column):
    """Extract the mapping between category names and codes from the dataset. There should be an interface
    on the categorical index type for this map, but I could not find it. """

    t = pd.DataFrame( {'codes': df[column].cat.codes, 'values': df[column]} ).drop_duplicates().sort_values('codes')
    return { e.codes:e.values for e in list(t.itertuples())}

def extract_categories(fspath):
    """
    Create a Metatab doc with the schema for a CHIS file, including the categorical values.

    :param fspath: Path to the stata file.
    :return:
    """

    dfs = pd.read_stata(fspath)
    itr = pd.read_stata(fspath, iterator=True)

    var_d = itr.variable_labels()

    # We ought to be able to just use the value_labels() method to get value labels, but
    # there are a lot of variables it doesn't return values for. For those, we'll use
    # make_cat_map, which analyzes the categorical values directly.

    columns = []
    for col_name in dfs.columns:

        col = {
            'name' : col_name,
            'description':var_d[col_name],
            'ordered': False,
            'values': {}
        }

        try:
            col['values'] = make_cat_map(dfs, col_name)
        except AttributeError as e:
            # Probably, column is not a category
            pass

        try:
            if dfs[col_name].cat.ordered:
                col['ordered'] = True
        except AttributeError as e:
            pass

        columns.append(col)

    return columns

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

        u_ = parse_app_url(source_url)
        self.target_file = u_.target_file
        self.target_segment = u_.target_segment

        self.value_type = u_.fragment_query.get('values', 'categorical')

        self.kwargs = kwargs

    def dataframe(self, *args, **kwargs):
        """Return a pandas dataframe from the resource"""

        import pandas as pd

        return pd.read_stata(self.ref.fspath, *args, **kwargs)

    @property
    def columns(self):
        import pandas as pd

        return extract_categories(self.ref.fspath)


    def __iter__(self):

        df = self.dataframe()

        if self.value_type == 'codes':
            df = to_codes(df)

        yield from iterate_pandas(df)
