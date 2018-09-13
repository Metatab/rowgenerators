# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

"""

from .file import FileUrl
from rowgenerators.appurl.web import WebUrl

from rowgenerators.appurl.archive.zip import ZipUrl

class ShapefileUrl(ZipUrl):

    match_priority = ZipUrl.match_priority - 1

    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)

        self.scheme_extension = 'shape'

        if not self.target_file:
            self.fragment = ['.*\.shp$',self.fragment[1]]

    @classmethod
    def _match(cls, url, **kwargs):
        return url.scheme_extension == 'shape'

    def get_resource(self):
        return super().get_resource()

    def list(self):
        """List the files in the referenced Zip file"""

        from zipfile import ZipFile

        real_files = ZipUrl.real_files_in_zf(ZipFile(self.fspath))
        return list(self.set_target_file(rf) for rf in real_files)

    def get_target(self):
        """Returns the ZIP file, with the correct fragment, not the inner
        file as a ZipUrl normally does. """


        if not str(self.fspath).endswith('.shp'):
            #Resolve the target_file, which may be a reg-ex
            self.fragment = [ZipUrl.get_file_from_zip(self), self.fragment[1]]

        return self

    def get_archive_target(self):
        """Returns the inner .shp file, like a Normal ZipUrl would"""

        self.fragment = [ZipUrl.get_file_from_zip(self), self.fragment[1]]

        t = super().get_target().clear_fragment()

        return ShapefileShpUrl(str(t))

    def geoframe(self, *args, **kwargs):

        import geopandas

        return self.get_resource().get_target().generator.geoframe( *args, **kwargs)

class ShapefileShpUrl(FileUrl):

    match_priority = 1000 # Don't match this one.

    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)

        self.scheme_extension = 'shape'

        self.fragment = None

    @classmethod
    def _match(cls, url, **kwargs):
        return False


    def get_resource(self):
        return self

    def get_target(self):
        """Returns the ZIP file, with the correct fragmentment, not the inner
        file as a ZipUrl normally does. """

        return self

    def get_archive_target(self):
        """Returns the inner .shp file, like a Normal ZipUrl would"""

        return self