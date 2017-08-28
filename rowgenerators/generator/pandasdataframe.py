# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


class PandasDataframeSource(Source):
    """Iterates a pandas dataframe  """

    def __init__(self, spec, df, cache, working_dir=None):
        super(PandasDataframeSource, self).__init__(spec, cache)

        self._df = df

    def __iter__(self):

        self.start()

        df = self._df

        if len(df.index.names) == 1 and df.index.names[0] == None:
            # For an unnamed, single index, assume that it is just a row number
            # and we don't really need it

            yield list(df.columns)

            for index, row in df.iterrows():
                yield list(row)

        else:

            # Otherwise, either there are more than

            index_names = [n if n else "index{}".format(i) for i,n in enumerate(df.index.names)]

            yield index_names + list(df.columns)

            if len(df.index.names) == 1:
                idx_list = lambda x: [x]
            else:
                idx_list = lambda x: list(x)

            for index, row in df.iterrows():
                yield idx_list(index) + list(row)


        self.finish()


