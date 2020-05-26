# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl.web.web import WebUrl
from rowgenerators.appurl.url import parse_app_url
from rowgenerators.appurl.file.csv import CsvFileUrl

class GoogleProtoCsvUrl(WebUrl):
    """Access a Google spreadheet as a CSV format download"""

    csv_url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'

    def __init__(self, url, **kwargs):
        kwargs['resource_format'] = 'csv'
        kwargs['encoding'] = 'utf8'
        kwargs['proto'] = 'gs'
        super(GoogleProtoCsvUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return url.proto == 'gs'

class GoogleSpreadsheetUrl(WebUrl):

    """A Google spreadsheet url. Supports URLs with these forms:

        gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w
        gs+https://drive.google.com/open?id=1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w
        gs+https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit?usp=sharing
        gs+https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit?usp=sharing
        gs+https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit#gid=801701031

        You can also leave off the 'gs+', so these are also valid:

        https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit?usp=sharing
        https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit?usp=sharing
        https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/edit#gid=801701031

        """

    match_priority = WebUrl.match_priority - 1

    url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'
    gid_siffix = '&gid={gid}'

    def __init__(self, url=None, downloader=None, **kwargs):

        super().__init__(url, downloader, **kwargs)

        parts = self.path.split('/') + [self.query.replace('id=', ''), self.netloc]
        parts = list(reversed(sorted(parts, key=lambda k: len(k))))
        self.key = parts[0]

        if self.scheme == 'gs':
            self.gid = self.target_file
        else:
            self.gid = self.fragment_query.get('gid')

        if self.gid:
            web_url = (self.url_template + self.gid_siffix).format(key=self.key, gid=self.gid)
        else:
            web_url = self.url_template .format(key=self.key)

        web_url +="#target_file={}-{}.csv".format(self.key,self.gid)

        self.web_url = WebUrl(web_url, downloader=self.downloader)

    @classmethod
    def _match(cls, url, **kwargs):

        return url.proto == 'gs' or \
               (url.netloc == 'docs.google.com' and url.path.startswith('/spreadsheets'))

    def get_resource(self):

        return CsvFileUrl(str(self.web_url.get_resource()))

    def get_target(self):
        return self.get_resource().get_target()




