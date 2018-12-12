from rowgenerators.appurl.file import FileUrl


class StataUrl(FileUrl):
    """
    """
    match_priority = 85

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'file' and url.target_format == 'dta'


    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)






