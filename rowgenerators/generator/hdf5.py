# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source
from rowgenerators.exceptions import RowGeneratorError
import pandas as pd
from functools import lru_cache

def bisect_df(df, v, col='variable_name_nd'):
    """Perform a binary search on a dataframe"""
    from math import log

    max_n = (log(len(df)) / log(2)) + 2

    low = 0
    high = len(df)
    mid = int((low + high) / 2)
    n = 0
    while True:

        e = df.iloc[mid]

        if v == e[col]:
            return e
        if v > e[col]:
            low = mid
        else:
            high = mid

        mid = int((low + high) / 2)

        if n > max_n:
            return None

        n += 1

        #print(n, max_n, v, low, mid, high)

    return None

@lru_cache()
def get_var_labels(ds_name, f):
    ds = f[ds_name+'_variable_labels']
    dhs = f[ds_name + '_variable_labels_headers']

    cols = dhs[:]
    return pd.DataFrame(ds[:], columns=cols)

class Hdf5Source(Source):
    """Generate rows from an excel file"""

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.url = ref

    def dataframe(self, limit=None, *args, **kwargs):
        import h5py
        import sys

        ds_name = self.ref.fragment[0]

        slice_frag = self.ref.fragment[1]

        with h5py.File(str(self.ref.fspath)) as f:

            ds = f[ds_name]

            try:
                if ':' in slice_frag:
                    parts = slice_frag.split(':')
                    slc = slice(int(parts[0]), int(parts[1]))
                else:
                    slc = [int(e) for e in slice_frag.split(',')]

                try:
                    headers_ds = f[ds_name + '_headers']
                    headers = list(headers_ds[slc])

                except KeyError:
                    headers = []

            except ValueError: # Hopefully b/c the slice is actually strings, not integers

                df = get_var_labels(ds_name, f)

                def name_to_num(v):
                    e = bisect_df(df,  v)
                    return int(e.col_no)

                if ':' in slice_frag:
                    slice_frag = slice_frag.split(':')
                    slc = slice(name_to_num(slice_frag[0]),name_to_num(slice_frag[1]))
                    headers = headers[slc]
                else:
                    slice_frag = slice_frag.split(',')
                    slc = [name_to_num(e) for e in slice_frag]
                    headers = slice_frag



            try:
                vars_ds = f[ds_name + '_variable_labels']
            except KeyError:
                headers = []

            headers_map = {i:e for i,e in enumerate(headers)}

            df = pd.DataFrame(ds[0:10,slc], columns=headers if headers else None)

        return df

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        raise NotImplementedError()