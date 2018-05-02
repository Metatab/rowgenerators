# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""
URLS for referencing python code.
"""

from .file import FileUrl


class PythonUrl(FileUrl):
    """
    URL to reference python code
    """

    match_priority = FileUrl.match_priority - 1

    def __init__(self, url=None, downloader=None, **kwargs):

        super().__init__(url, downloader, **kwargs)
        kwargs['scheme'] = 'python'

    @classmethod
    def _match(cls, url, **kwargs):
        return url.proto == 'python'

    def get_resource(self):
        return self

    def get_target(self):
        return self

    @property
    def object(self):
        """Return the python thing, a class or a function, that will be invoked """

        from rowgenerators.appurl.util import import_name_or_class

        path = self.path.replace('/','.')+'.'+self.target_file

        return import_name_or_class(path)

    def __call__(self, *args, **kwargs):

        try:
            return self.object(*args, **kwargs)
        except TypeError as e:
            raise TypeError(str(e)+"; for args='{}', kwargs='{}' ".format(args, kwargs))



