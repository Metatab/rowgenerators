# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl import Url

class WebPageUrl(Url):
    """A URL for webpages, not for data"""

    def __init__(self, url=None, downloader=None, **kwargs):
        raise NotImplementedError()
        super().__init__(url, downloader, **kwargs)
