
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from .file import FileUrl

class Hdf5Url(FileUrl):
    """URL that references an HDF5 file"""

    match_priority = FileUrl.match_priority-5

    def __init__(self, url=None, downloader=None, **kwargs):
        super().__init__(url, downloader, **kwargs)

    @classmethod
    def _match(cls, url, **kwargs):
        return url.resource_format in ('h5', 'hdf5')

    @property
    def target_format(self):
        return 'h5'

    def list(self, list_self=False):
        import h5py

        with h5py.File(str(self.fspath)) as f:
            return list(f.keys())

    @property
    def generator(self):
        """
        Return the generator for this URL, if the rowgenerator package is installed.

        :return: A row generator object.
        """

        from rowgenerators.core import get_generator

        return get_generator(self, source_url=self)
