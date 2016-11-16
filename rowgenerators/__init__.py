# -*- coding: utf-8 -*-

from generators import  CsvSource, TsvSource, FixedSource, PartitionSource, ExcelSource,\
    GoogleSource, GeneratorSource, MPRSource, AspwCursorSource, PandasDataframeSource, \
    SocrataSource
from .exceptions import SourceError

from .util import  DelayedOpen, DelayedDownload
from sourcespec import SourceSpec, RowGenerator
from rowproxy import  RowProxy, GeoRowProxy

__all__ = [
    RowGenerator,
    SourceError, CsvSource, TsvSource, FixedSource, PartitionSource,
    ExcelSource, GoogleSource, AspwCursorSource, SocrataSource,
    DelayedOpen, DelayedDownload, RowProxy, GeoRowProxy, GeneratorSource]

try:
    # Only if the underlying fiona and shapely libraries are installed with the [geo] extra
    from .accessors import  ShapefileSource
    __all__.append('ShapefileSource')
except ImportError:
    pass
