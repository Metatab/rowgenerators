# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Base class for file URLs, URLs on a local file system. These are URLs that can be opened and read"""

from rowgenerators.appurl.url import Url
from rowgenerators.appurl.util import ensure_dir
from os.path import exists, isdir, dirname, basename, join


class FileUrl(Url):
    """FileUrl is the baseclass for URLs that reference a general file, assumed to be
    local to the file system.

    This documentation only describes the differences in implementation from the super class.
    See the documentation for the superclass, :py:class:`appurl.Url` for the default implementations.

    """

    def __init__(self, url=None, downloader=None,**kwargs):
        """
        """
        super().__init__(url, downloader=downloader, **kwargs)

    match_priority = 90

    def exists(self):
        return exists(self.path)

    def isdir(self):
        return isdir(self.path)

    def dirname(self):
        return dirname(self.path)

    def basename(self):
        return basename(self.path)

    def ensure_dir(self):
        ensure_dir(self.path)



    def list(self):
        """List the contents of a directory

        """

        if self.isdir():
            from os import listdir

            return [u for e in listdir(self.path) for u in self.join(e).list()]

        else:
            return [self]



    def get_resource(self):
        """Return a url to the resource, which for FileUrls is always ``self``."""

        return self

    def get_target(self):
        """Return the url of the target file in the local file system.
        """

        t =  self.clear_fragment()

        if self.encoding:
            t.encoding = self.encoding


        return t

    def read(self, mode='rb'):
        """Return contents of the target file"""
        with open(self.get_target().path, mode=mode) as f:
            return f.read()

    def join_target(self, tf):
        """For normal files, joining a target assumes the target is a child of the current target's
        directory, so this just passes through the :py:meth:`Url.join_dir`"""

        try:
            tf = tf.path
        except:
            pass

        return self.clone().join_dir(tf)

    def rename(self, new_path):
        from os import rename

        rename(self.path, new_path)

        self.path = new_path

    def base_rename(self, new_name):
        """"Rename only the last path element"""

        new_path = join(dirname(self.path), new_name)

        return self.rename(new_path)


