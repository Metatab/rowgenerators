# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from itertools import islice


class Source(object):
    """Base class for accessors that generate rows from any source

    Subclasses of Source must override at least _get_row_gen method.
    """

    priority = 100

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        self.ref = ref

        self.cache = cache

    @property
    def headers(self):
        """Return a list of the names of the columns of this file, or None if the header is not defined.

        This should *only* return headers if the headers are unambiguous, such as for database tables,
        or shapefiles. For other files, like CSV and Excel, the header row can not be determined without analysis
        or specification."""

        return None

    @headers.setter
    def headers(self, v):
        """Catch attempts to set"""
        raise NotImplementedError

    @property
    def columns(self):
        """ Returns columns for the file accessed by accessor.

        """

        return None

    @property
    def meta(self):
        return {}

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        raise NotImplementedError()

    @property
    def iter_rp(self):
        """Iterate, yielding row proxy objects rather than rows"""

        from .rowproxy import RowProxy

        itr = iter(self)

        headers = next(itr)

        row_proxy = RowProxy(headers)

        for row in itr:
            yield row_proxy.set_row(row)

    @property
    def iter_dict(self):
        """Iterate, yielding dicts rather than rows"""

        itr = iter(self)

        headers = next(itr)

        for row in itr:
            yield dict(zip(headers, row))


    def start(self):
        pass

    def finish(self):
        pass



class SelectiveRowGenerator(object):
    """Proxies an iterator to remove headers, comments, blank lines from the row stream.
    The header will be emitted first, and comments are avilable from properties """

    def __init__(self, seq, start=0, headers=[], comments=[], end=[], load_headers=True, **kwargs):
        """
        An iteratable wrapper that coalesces headers and skips comments

        :param seq: An iterable
        :param start: The start of data row
        :param headers: An array of row numbers that should be coalesced into the header line, which is yieled first
        :param comments: An array of comment row numbers
        :param end: The last row number for data
        :param kwargs: Ignored. Sucks up extra parameters.
        :return:
        """

        self.iter = iter(seq)
        self.start = start if (start or start is 0) else 1
        self.header_lines = headers if isinstance(headers, (tuple, list)) else [int(e) for e in headers.split(',') if e]
        self.comment_lines = comments
        self.end = end

        self.load_headers = load_headers

        self.headers = []
        self.comments = []

        int(self.start)  # Throw error if it is not an int

    @property
    def coalesce_headers(self):
        """Collects headers that are spread across multiple lines into a single row"""

        import re

        if not self.headers:
            return None

        header_lines = [list(hl) for hl in self.headers if bool(hl)]

        if len(header_lines) == 0:
            return []

        if len(header_lines) == 1:
            return header_lines[0]

        # If there are gaps in the values of a line, copy them forward, so there
        # is some value in every position
        for hl in header_lines:
            last = None
            for i in range(len(hl)):
                hli = str(hl[i])
                if not hli.strip():
                    hl[i] = last
                else:
                    last = hli

        headers = [' '.join(str(col_val).strip() if col_val else '' for col_val in col_set)
                   for col_set in zip(*header_lines)]

        headers = [re.sub(r'\s+', ' ', h.strip()) for h in headers]

        return headers

    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i in self.header_lines:
                if self.load_headers:
                    self.headers.append(row)
            elif i in self.comment_lines:
                self.comments.append(row)
            elif i == self.start:
                break

        if self.headers:

            headers = self.coalesce_headers
            yield headers
        else:
            # There is no header, so fake it

            headers = ['col' + str(i) for i, _ in enumerate(row)]

        yield row

        for row in self.iter:
            yield row
