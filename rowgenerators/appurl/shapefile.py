# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

"""


from appurl import ZipUrl

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

    def get_target(self):

        #Resolve the target_file, which may be a reg-ex
        self.fragment = [ZipUrl.get_file_from_zip(self), self.fragment[1]]

        return self






