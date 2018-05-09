
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from .file import FileUrl

class ExcelFileUrl(FileUrl):
    """URL that references an Excl file, either .xls or .xlsx"""

    match_priority = FileUrl.match_priority-5

    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'file' and url.resource_format in ('xlsx', 'xls')

    @property
    def resource_url(self):
        from rowgenerators.appurl.util import unparse_url_dict

        return unparse_url_dict(self.dict,
                                scheme_extension=False,
                                fragment=False)

    @property
    def target_file(self):
        # This one should probably thow an exception, to force user to use target_segment.
        return self.fragment[0]

    @property
    def target_segment(self):
        return self.fragment[1] if self.fragment[1] else self.fragment[0]

    @property
    def target_format(self):
        return 'xlsx'

    def list(self, list_self=False):

        from xlrd import open_workbook

        wb = open_workbook(filename=str(self.fspath))

        def _l():
            return list(self.set_target_segment(sheet) for sheet in wb.sheet_names())

        return ( [self] if list_self else [] ) + _l()



    def join(self, s):
        return super().join(s)

    def join_dir(self, s):
        return super().join_dir(s)

    def join_target(self, tf):

        try:
            tf = tf.path
        except:
            pass

        u = self.clone()
        u.fragment = [tf,None] # In case its a tuple, don't edit in place
        return u

    def get_target(self):
        return self # Like super method, but don't clear the fragment; it's needed in the row generator








