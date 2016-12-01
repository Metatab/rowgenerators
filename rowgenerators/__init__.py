# -*- coding: utf-8 -*-

from generators import *
from .exceptions import SourceError

from .util import  DelayedOpen, DelayedDownload
from sourcespec import SourceSpec
from generators import RowGenerator
from rowproxy import RowProxy, GeoRowProxy

__all__ = [
    RowGenerator,
    SourceError, CsvSource, TsvSource, FixedSource, PartitionSource,
    ExcelSource, GoogleSource, AspwCursorSource, SocrataSource,
    DelayedOpen, DelayedDownload, RowProxy, GeoRowProxy, GeneratorSource, SelectiveRowGenerator]

try:
    # Only if the underlying fiona and shapely libraries are installed with the [geo] extra
    from .accessors import  ShapefileSource
    __all__.append('ShapefileSource')
except ImportError:
    pass
