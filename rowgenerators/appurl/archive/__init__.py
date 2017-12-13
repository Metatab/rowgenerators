# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""
Archive Urls represent an archive file in the local file system, such as a Zip file.
They provide implementations of :py:meth:`Url.get_target` that will extract the
target file from the archive and store it in the file cache, returning a new Url to
the extracted file."""


from .zip import ZipUrl

__all__ = ["ZipUrl"]