# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from os.path import basename, join
from rowgenerators.appurl.web.web import WebUrl

class SocrataUrl(WebUrl):
    """Url to represent a dataset stored in a Socrata data repository. """

    def __init__(self, url=None,downloader=None, **kwargs):

        kwargs['resource_format'] = 'csv'
        kwargs['encoding'] = 'utf8'
        kwargs['proto'] = 'socrata'

        super().__init__(url,downloader=downloader, **kwargs)



    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'socrata'


    @property
    def resource_url(self):
        from rowgenerators.appurl.util import unparse_url_dict

        return unparse_url_dict(self.__dict__,
                                scheme_extension=False,
                                fragment=False,
                                path=join(self.path, 'rows.csv'))

    @property
    def resource_file(self):
        return basename(self.path)+'.csv'



    def get_resource(self):
        """Get the contents of resource and save it to the cache, returning a file-like object"""
        raise NotImplementedError()

    def get_target(self):
        """Get the contents of the target, and save it to the cache, returning a file-like object
        :param downloader:
        """
        raise NotImplementedError()
