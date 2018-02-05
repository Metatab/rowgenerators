# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


class DictRowGenerator(object):
    """Constructed on a RowGenerator, returns dicts from the second and subsequent rows, using the
    first row as dict keys. """

    def __init__(self, rg):
        self._rg = rg

    def __iter__(self):


        headers = None

        for row in self._rg:
            if not headers:
                headers = [str(e).strip() for e in row]
                continue

            yield dict(zip(headers, row))

