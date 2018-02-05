# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
from rowgenerators.exceptions import SourceError, RowGeneratorError
from rowgenerators.source import Source

class PythonSource(Source):
    """Generate rows from a callable object. Takes kwargs from the spec to pass into the program. """

    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):

        super().__init__(ref, cache, working_dir, **kwargs)

        if not working_dir in sys.path:

            sys.path.append(working_dir)

        self.env = env or {}

        self.kwargs = kwargs


    def __iter__(self):
        try:
            yield from self.ref(env=self.env, cache=self.cache, **self.kwargs)
        except TypeError as e:
            # call to Python Url has wrong signature

            RowGeneratorError(str(e))