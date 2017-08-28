
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import sys
import inspect
from rowgenerators.source import Source


class IteratorSource(Source):
    def __init__(self, itr, cache=None, working_dir=None):
        super().__init__(itr, cache, working_dir)

        self.itr = itr

        if inspect.isfunction(self.gen):
            self.gen = self.gen()

    def __iter__(self):
        """ Iterate over all of the lines in the generator. """

        self.start()

        yield from self.itr

        self.finish()

