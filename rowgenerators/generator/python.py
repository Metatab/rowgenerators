# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys

from rowgenerators import Source
from rowgenerators.exceptions import SourceError, RowGeneratorError
from rowgenerators.source import Source
import types
from itertools import islice


class PythonSource(Source):
    """Generate rows from a callable object. Takes kwargs from the spec to pass into the program.
    If the callable returns a pandas dataframe, iterate it as a PandasDataframeSource """

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):

        super().__init__(ref, cache, working_dir, **kwargs)

        if not working_dir in sys.path:

            sys.path.append(working_dir)

        self.env = env or {}

        self.kwargs = kwargs

    def __call__(self):
        '''Call the referenced function and return the result '''
        return self.ref(env=self.env, cache=self.cache, **self.kwargs)

    def dataframe(self, *args, **kwargs):

        from pandas import DataFrame

        o = self()

        if isinstance(o, types.GeneratorType):

            # Just normal data, so use the iterator in this object.
            headers = next(islice(self, 0, 1))
            data = islice(self, 1, None)

            return DataFrame(list(data), columns=headers)

        elif isinstance(o, DataFrame):
            return o

        else:
            return super().dataframe(*args, **kwargs)

    def __iter__(self):
        """Iterate rows from the python generator returned from the referenced function. Or, if the
        function returns a dataframe, yield from a PandasDataframeSource constructed on the dataframe"""

        o = self()

        if isinstance(o,types.GeneratorType):

            try:
                yield from o
            except TypeError as e:
                # call to Python Url has wrong signature

                raise RowGeneratorError(str(e))

        else:
            from metapack import get_cache

            yield from PandasDataframeSource('<df>', o, get_cache())


class PandasDataframeSource(Source):
    """Iterates a pandas dataframe  """


    def __init__(self, url, df, cache, working_dir=None, **kwargs):
        super().__init__(url, cache, working_dir, **kwargs)

        self._df = df

    def __iter__(self):

        import numpy as np

        self.start()

        df = self._df

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


        self.finish()