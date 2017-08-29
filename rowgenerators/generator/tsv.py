# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from .csv import CsvSource

class TsvSource(CsvSource):
    """Generate rows from a TSV (tab separated value) source"""

    delimiter = '\t'


