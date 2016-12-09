# -*- coding: utf-8 -*-

"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class RowGeneratorError(Exception):
    pass


class SourceError(RowGeneratorError):
    pass


class SpecError(RowGeneratorError):
    pass


class ConfigurationError(RowGeneratorError):
    pass


class DownloadError(RowGeneratorError):
    pass


class MissingCredentials(RowGeneratorError):
    pass
