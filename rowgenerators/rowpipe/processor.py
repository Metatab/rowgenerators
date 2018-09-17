# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

Row generating row processor

"""

from collections import defaultdict
from rowgenerators.rowpipe.codegen import make_row_processors, exec_context


class RowProcessor(object):
    """
    """

    def __init__(self, source, dest_table, source_headers=None, env=None, manager=None, code_path=None):

        """

        :param source: Row generating soruce
        :param dest_table: Destination table
        :param source_headers:
        :param env:
        :param env: A higher-level controller object, to be referenced from user-written transforms.
        :return:
        """

        self.source = source
        self.manager = manager
        self.source_headers = source_headers if source_headers is not None else self.source.headers
        self.dest_table = dest_table
        self.code_path = code_path

        self.env = exec_context()

        if env is not None:
            self.env.update(env)

        self.env['source'] = self.source
        self.env['pipe'] = None

        self.scratch = {}
        self.accumulator = {}
        self.errors = defaultdict(set)

        self.code = make_row_processors(self.source_headers, self.dest_table, env=self.env)

        self.code_path = self.write_code()

        exec (compile(self.code, self.code_path, 'exec'), self.env)

        self.procs = self.env['row_processors']

    def write_code(self):
        import hashlib
        import os
        import tempfile

        if self.code_path:
            path = self.code_path

        else:
            tf = tempfile.NamedTemporaryFile(prefix="rowprocessor-",
                                             suffix='{}.py'.format(hashlib.md5(self.code.encode('utf-8')).hexdigest()),
                                             delete=False)
            path = tf.name
            tf.close()

        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        with open(path,'w') as f:
            f.write(self.code)

        return path

    @property
    def headers(self):
        """Return a list of the names of the columns of this file, or None if the header is not defined.

        This should *only* return headers if the headers are unambiguous, such as for database tables,
        or shapefiles. For other files, like CSV and Excel, the header row can not be determined without analysis
        or specification."""

        return self.dest_table.headers

    @headers.setter
    def headers(self, v):
        raise NotImplementedError()

    @property
    def meta(self):
        return {}

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        from rowgenerators.rowproxy import RowProxy

        self.start()

        pipe = self.env['pipe']

        rp1 = RowProxy(self.source_headers) # The first processor step uses the source row structure
        rp2 = RowProxy(self.dest_table.headers) # Subsequent steps use the dest table

        for i, row in enumerate(self.source):

            try:
                rp = rp1

                for proc in self.procs:
                    row = proc(rp.set_row(row), i, self.errors, self.scratch, self.accumulator,
                               pipe, self.manager, self.source)

                    # After the first round, the row has the destination headers.
                    rp = rp2

                yield row
            except Exception as e:
                raise

        self.finish()

    def start(self):
        pass

    def finish(self):
        pass