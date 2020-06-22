# -*- coding: utf-8 -*-

"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

class RowGeneratorError(Exception):
    pass

class RowGeneratorConfigError(RowGeneratorError):
    """Row generator errors that result from the configuration of the rowgenerator,
    such as using a tab-based parser on a comma-based file"""

    def __init__(self, config_type: str, *args: object) -> None:
        super().__init__(*args)
        self.config_type = config_type

class SourceError(RowGeneratorError):
    pass

class DownloadError(SourceError):
    pass

class AccessError(DownloadError):
    """Got an acess error on download"""
    pass

class TextEncodingError(SourceError):
    pass

class SpecError(RowGeneratorError):
    pass


class ConfigurationError(RowGeneratorError):
    pass


class MissingCredentials(RowGeneratorError):
    pass

class SchemaError(RowGeneratorError):
    pass


class AppUrlError(Exception):
    pass