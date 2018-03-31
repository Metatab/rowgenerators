# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl.web.web import WebUrl


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

    def _process_resource_url(self):

        raise NotImplementedError

        self._process_fragment()

        # noinspection PyUnresolvedReferences
        self.resource_url = self.csv_url_template.format(
            key=self.parts.netloc)  # netloc is case-sensitive, hostname is forced lower.

        self.resource_file = self.parts.netloc

        if self.target_segment:
            self.resource_url += "&gid={}".format(self.target_segment)
            self.resource_file += '-' + self.target_segment

        self.resource_file += '.csv'

        if self.resource_format is None:
            self.resource_format = file_ext(self.resource_file)

        self.target_file = self.resource_file  # _process_target() file will use this self.target_file

    def component_url(self, s):

        raise NotImplementedError


        sp = parse_url_to_dict(s)

        if sp['netloc']:
            return s

        return reparse_url(self.url, fragment=s)

        url = reparse_url(self.resource_url, query="format=csv&gid=" + s)
        assert url
        return url



class GoogleSpreadsheetUrl(WebUrl):

    match_priority = WebUrl.match_priority - 1

    url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'
    gid_siffix = '&gid={gid}'

    def __init__(self, url=None, downloader=None, **kwargs):
        from rowgenerators.appurl.url import parse_app_url

        super().__init__(url, downloader, **kwargs)

        self._proto = 'gs'

        self.key = self.path or self.netloc # former without '://', later with ':'
        self.gid = self.target_file

        if self.gid:
            web_url = (self.url_template + self.gid_siffix).format(key=self.key, gid=self.gid)
        else:
            web_url = self.url_template .format(key=self.key)

        web_url +="#target_file={}-{}.csv".format(self.key,self.gid)

        self.web_url = parse_app_url(web_url)

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'gs'

    def get_resource(self):

        from rowgenerators.appurl.file.csv import CsvFileUrl
        return CsvFileUrl(str(self.web_url.get_resource()))

    def get_target(self):
        return self.get_resource().get_target()

