# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from  rowgenerators.appurl import Url

class ApplicationUrl(Url):
    """Application URLs have weirdo schemes or protos"""

    reparse = False

    def __init__(self, url, **kwargs):
        raise NotImplementedError()
        super(ApplicationUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) not in ['file', 'ftp', 'http', 'https']

