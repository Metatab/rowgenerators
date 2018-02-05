# Copyright (c) 2018 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Appurls or sql databases"""

from rowgenerators.appurl import Url

class Sql(Url):

    """"""

    match_priority = Url.match_priority - 1

    def __init__(self, url=None, downloader=None, **kwargs):
        # Save for auth_url()


        self.username

        self._orig_url = url
        self._orig_kwargs = dict(kwargs.items())

        super().__init__(url,downloader=downloader, **kwargs)

    @property
    def dsn(self):
        """Return a database connection string. The string will have values interpolated from
        the environment"""

        import os

        u = self.clone()
        u.password = u.password.format(**os.environ)
        u.fragment = []

        return str(u)




class OracleSql(Sql):

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'oracle'


