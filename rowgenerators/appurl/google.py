
from appurl import WebUrl, parse_app_url
from appurl.file.csv import CsvFileUrl

class GoogleSpreadsheetUrl(WebUrl):

    match_priority = WebUrl.match_priority - 1

    url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'
    gid_siffix = '&gid={gid}'

    def __init__(self, url=None, downloader=None, **kwargs):
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
        return CsvFileUrl(str(self.web_url.get_resource()))

    def get_target(self):
        return self.get_resource().get_target()

