# Copyright (c) 2018 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Appurls or sql databases"""

from rowgenerators.appurl import Url
from rowgenerators.exceptions import SourceError, RowGeneratorError

class Sql(Url):

    """"""

    match_priority = Url.match_priority - 1

    def __init__(self, url=None, downloader=None, **kwargs):
        # Save for auth_url()


        self.username

        self._orig_url = url
        self._orig_kwargs = dict(kwargs.items())

        super().__init__(url,downloader=downloader, **kwargs)

    def get_resource(self):
        return self

    def get_target(self):
        return self

    @property
    def dsn(self):
        """Return a database connection string. The string will have values interpolated from
        the environment"""

        import os

        u = self.clone()
        u.fragment = []
        try:
            u.password = u.password.format(**os.environ)
        except KeyError:
            raise RowGeneratorError(f"Failed to set password from environment variable in connection string '{str(u)}' ")

        return str(u)

    @property
    def sql(self):
        """Return the query, which is embedded in the fragment"""

        return self._fragment[0]


class OracleSql(Sql):

    generator_class = Sql

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'oracle'


