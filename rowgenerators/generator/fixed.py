# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source

class FixedSource(Source):
    """Generate rows from a fixed-width source"""

    def __init__(self, ref, table=None, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir, **kwargs)

        self.table = table

        assert self.table

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        parse = self.table.make_fw_row_parser()


        with open(self.ref.fspath) as f:
            for line in f.readlines():
                yield parse(line)

        self.finish()

