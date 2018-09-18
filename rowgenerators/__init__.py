# -*- coding: utf-8 -*-

from .appurl.url import parse_app_url, Url
from .appurl.util import get_cache, set_default_cache_name
from .core import get_generator
from .source import  Source, RowGenerator
from .appurl.web.download import Downloader
from .exceptions import SourceError


from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass


