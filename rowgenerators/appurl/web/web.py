# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Base class for Web URLs. These are URLs that can be fetched to the local filesystem. """


from rowgenerators.appurl.util import parse_url_to_dict
from rowgenerators.appurl.url import Url

class WebUrl(Url):
    """Base class for web urls.

    This documentation only describes the differences in implementation from the super class.
    See the documentation for the superclass, :py:class:`appurl.Url` for the default implementations.

    """

    match_priority = 20

    def __init__(self, url=None, downloader=None, **kwargs):

        super().__init__(url,downloader=downloader, **kwargs)

        self._resource = None # return value from the downloader

    @classmethod
    def _match(cls, url, **kwargs):
        """Return True if this handler can handle the input URL"""
        return url.scheme.startswith('http')

    def list(self):
        """Return a list of this URL with the fragments from listing the resource"""

        r = self.get_resource()

        return list(self.set_fragment(u.fragment) for u in r.list())


    @property
    def auth_resource_url(self):
        """Return An ``S3:`` version of the url, with a resource_url format that will trigger boto auth"""

        # This is just assuming that the url was created as a resource from the S2Url, and
        # has the form 'https://s3.amazonaws.com/{bucket}/{key}'

        parts = parse_url_to_dict(self.resource_url)

        return 's3://{}'.format(parts['path'])

    def get_resource(self):
        """Get the contents of resource and save it to the cache, returning a file-like object"""
        from rowgenerators import parse_app_url # Here, to break an import cycle

        self._resource = self._downloader.download(self.inner)


        ru = parse_app_url(self._resource.sys_path,
                           fragment=self.fragment,
                           fragment_query=self.fragment_query,
                           scheme_extension=self.scheme_extension,
                           target_format=self._target_format,
                           downloader = self.downloader
                           )

        return ru

    def dirname(self):
        from os.path import dirname
        return dirname(self.path)

    def basename(self):
        from os.path import basename
        return basename(self.path)

    def join_dir(self, s):


        if self.resource_format in ('zip','xlsx'):
            u = Url(s)
            return self.clone(fragment=u.path)
        else:
            return super().join_dir(s)

    def join_target(self, tf):

            try:
                tf = str(tf.path)
            except:
                pass

            if self.target_format:
                u = self.clone()
                u.fragment = [tf, self.fragment[1]]

            else:
                # Assuming that if there is no target format, there is no actual target file
                # and the URL is specifying a directory.
                u = self.join(tf)

            return u

class FtpUrl(WebUrl):

    @classmethod
    def _match(cls, url, **kwargs):
        """Return True if this handler can handle the input URL"""
        return url.scheme.startswith('ftp')


