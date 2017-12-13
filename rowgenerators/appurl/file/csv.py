# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from .file import FileUrl
from os.path import basename
from rowgenerators.appurl.util import unparse_url_dict, file_ext

class CsvFileUrl(FileUrl):
    """URL that references a CSV file"""

    match_priority = FileUrl.match_priority - 5

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'file' and url.target_format == 'csv'


    @property
    def resource_url(self):
        return unparse_url_dict(self.__dict__,
                                scheme=self.scheme if self.scheme else 'file',
                                scheme_extension=False,
                                fragment=False)

    @property
    def target_format(self):
        return 'csv'

