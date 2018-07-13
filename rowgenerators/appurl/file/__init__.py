# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

"""
File URLs represent resources that are acessible on the local file system.

Because these URLs are assumed to be local, the resource already exists on
the local file system, and the :py:meth:`FileUrl.get_resource()` just returns ``self``

"""


from .file import FileUrl
from .program import ProgramUrl
from .python import PythonUrl
from .csv import CsvFileUrl
from .excel import ExcelFileUrl
#from .shapefile import ShapefileShpUrl, ShapefileUrl