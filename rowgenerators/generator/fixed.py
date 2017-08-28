# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def __init__(self, spec, dflo, cache, working_dir=None):
        """

        Args:
            spec (sources.SourceSpec): specification of the source.
            fstor (sources.util.DelayedOpen):

        """

        super(FixedSource, self).__init__(spec, dflo, cache)

    def make_fw_row_parser(self):

        parts = []

        if not self.spec.columns:
            raise SourceError('Fixed width source must have a schema defined, with column widths.')

        for i, c in enumerate(self.spec.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise SourceError('Fixed width source {} must have start and width values for {} column '
                                  .format(self.spec.name, c.name))

            parts.append('row[{}:{}]'.format(c.start - 1, c.start + c.width - 1))

        code = 'lambda row: [{}]'.format(','.join(parts))

        return eval(code)

    @property
    def headers(self):
        return [c.name if c.name else i for i, c in enumerate(self.spec.columns)]

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        parser = self.make_fw_row_parser()

        for line in self._fstor.open(mode='r', encoding=self.spec.encoding):
            yield [e.strip() for e in parser(line)]

        self.finish()

