
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
import inspect
from rowgenerators.source import Source


class IteratorSource(Source):
    def __init__(self, ref, cache=None, working_dir=None, env=None, **kwargs):
        super().__init__(ref, cache, working_dir)

        self.itr = ref

        if inspect.isfunction(self.itr):
            self.itr = self.itr()

    def __iter__(self):
        """ Iterate over all of the lines in the generator. """

        self.start()

        yield from self.itr

        self.finish()

