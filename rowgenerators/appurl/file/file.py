# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""Base class for file URLs, URLs on a local file system. These are URLs that can be opened and read"""

from rowgenerators.appurl.url import Url
from rowgenerators.appurl.util import ensure_dir
from os.path import exists, isdir, dirname, basename, join
import pathlib
from urllib.parse import unquote

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

        # For resolving relative paths
        self.working_dir = self._kwargs.get('working_dir')



    match_priority = 90

    def exists(self):
        return exists(self.fspath)

    def isdir(self):
        return isdir(self.fspath)

    def dirname(self):
        return dirname(self.fspath)

    def basename(self):
        return basename(self.fspath)

    def ensure_dir(self):
        ensure_dir(self.fspath)

    def join(self, s):
        return super().join(s)

    @property
    def fspath(self):
        import pathlib
        import re

        p  = unquote(self._path)

        if self.netloc: # Windows UNC name
            return pathlib.PureWindowsPath("//{}{}".format(self.netloc,p))

        elif re.match('[a-zA-Z]:', p): # Windows absolute path
            return pathlib.PureWindowsPath(unquote(p))

        else:
            return pathlib.Path(pathlib.PurePosixPath(p))

    @property
    def path_is_absolute(self):
        return self.fspath.is_absolute()

    def list(self):
        """List the contents of a directory
        """

        if self.isdir():
            from os import listdir

            return [u for e in listdir(self.fspath) for u in self.join(e).list()]

        else:
            return [self]

    def get_resource(self):
        """Return a url to the resource, which for FileUrls is always ``self``."""

        return self

    def get_target(self):
        """Return the url of the target file in the local file system.
        """
        from os.path import isabs, join, normpath

        t = self.clear_fragment()

        if self.encoding:
            t.encoding = self.encoding

        if not isabs(t.fspath) and self.working_dir:
            t.path = normpath(join(self.working_dir, t.fspath))

        return t

    def read(self, mode='rb'):
        """Return contents of the target file"""

        path = self.get_resource().get_target().fspath

        with open(path, mode=mode) as f:
            return f.read()

    def join_target(self, tf):
        """For normal files, joining a target assumes the target is a child of the current target's
        directory, so this just passes through the :py:meth:`Url.join_dir`"""

        try:
            tf = str(tf.path)
        except:
            pass

        return self.clone().join_dir(tf)

    def rename(self, new_path):
        from os import rename

        rename(self.fspath, new_path)

        self.path = new_path

    def base_rename(self, new_name):
        """"Rename only the last path element"""

        new_path = join(dirname(self.fspath), new_name)

        return self.rename(new_path)

    def __str__(self):
        return super().__str__()


