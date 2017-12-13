# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl import Url


class CkanUrl(Url):
    def __init__(self, url, **kwargs):
        raise NotImplementedError()
        kwargs['proto'] = 'ckan'
        super(CkanUrl, self).__init__(url, **kwargs)



