# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


class DataRowGenerator(object):
    """Returns only rows between the start and end lines, inclusive """

    def __init__(self, seq, start=0, end=None, **kwargs):
        """
        An iteratable wrapper that coalesces headers and skips comments

        :param seq: An iterable
        :param start: The start of data row
        :param end: The last row number for data
        :param kwargs: Ignored. Sucks up extra parameters.
        :return:
        """

        self.iter = iter(seq)
        self.start = start
        self.end = end
        self.headers = []  # Set externally

        int(self.start)  # Throw error if it is not an int
        assert self.start > 0

    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i < self.start or (self.end is not None and i > self.end):
                continue

            yield row

