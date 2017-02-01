# -*- coding: utf-8 -*-

from .generators import *
from .exceptions import SourceError

from .util import DelayedFlo, get_cache
from .sourcespec import SourceSpec
from .urls import decompose_url
from .rowproxy import RowProxy, GeoRowProxy
from .fetch import enumerate_contents, inspect
from .exceptions import *
import sys

__all__ = [k for k in sys.modules[__name__].__dict__.keys()
           if not k.startswith('_') and
           k not in ('sys','sourcespec','six','exceptions','fetch','rowproxy','util', 'generators')]

try:
    # Only if the underlying fiona and shapely libraries are installed with the [geo] extra
    from .accessors import ShapefileSource
    __all__.append('ShapefileSource')
except ImportError:
    pass
