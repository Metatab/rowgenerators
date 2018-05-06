# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from rowgenerators.source import Source

##
## FIXME This is probably broken
## But it's Socrata, so who really cares?

class SocrataSource(Source):
    """Iterates a CSV soruce from the JSON produced by Socrata  """

    def __init__(self, spec, dflo, cache, working_dir=None, env=None):

        super(SocrataSource, self).__init__(spec, cache)

        self._socrata_meta = None

        self._download_url = spec.url + '/rows.csv'

        self._csv_source = CsvSource(spec, dflo)

    @classmethod
    def download_url(cls, spec):
        return spec.url + '/rows.csv'

    @property
    def _meta(self):
        """Return the Socrata meta data, as a nested dict"""
        import requests

        if not self._socrata_meta:
            r = requests.get(self.spec.url)

            r.raise_for_status()

            self._socrata_meta = r.json()

        return self._socrata_meta

    @property
    def headers(self):
        """Return headers.  """

        return [c['fieldName'] for c in self._meta['columns']]

    datatype_map = {
        'text': 'str',
        'number': 'float',
    }

    @property
    def meta(self):
        """Return metadata """

        return {
            'title': self._meta.get('name'),
            'summary': self._meta.get('description'),
            'columns': [
                {
                    'name': c.get('fieldName'),
                    'position': c.get('position'),
                    'description': c.get('name') + '.' + c.get('description'),

                }
                for c in self._meta.get('columns')
                ]

        }

    def __iter__(self):

        self.start()

        for i, row in enumerate(self._csv_source):
            # if i == 0:
            #    yield self.headers

            yield row

        self.finish()

