from rowgenerators.appurl.file import FileUrl
from rowgenerators.exceptions import AppUrlError
from publicdata.census.util import sub_geoids, sub_summarylevel
from warnings import warn


class StataUrl(FileUrl):
    """
    """
    match_priority = 85

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'file' and url.target_format == 'dta'


    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)






