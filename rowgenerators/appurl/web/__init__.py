# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""
WebUrls represent resources that are accessible on the web. They provide implementations of
:py:meth:`Url.get_resource` that will download a file from the web and store it in the file
cache, returning a new Url object pointing to the downloaded file.
"""

from .web import WebUrl
from .s3 import S3Url
from .socrata import SocrataUrl
from .download import Downloader

__all__ = "WebUrl Downloader S3Url SocrataUrl ".split()