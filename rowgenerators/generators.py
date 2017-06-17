"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


import six

from rowgenerators.fetch import download_and_cache, get_file_from_zip
from rowgenerators.util import real_files_in_zf, DelayedFlo

from .exceptions import TextEncodingError, SourceError

from .sourcespec import SourceSpec
from os.path import exists,normpath
from os import environ
import json

from rowgenerators.util import fs_join as join


custom_proto_map = {}
custom_type_map = {}

def register_proto(proto, clz):
    custom_proto_map[proto] = clz

def register_type(name, clz):
    custom_type_map[name] = clz

def PROTO_TO_SOURCE_MAP():

    d =  {
        'program': ProgramSource,
        'ipynb': NotebookSource,
        'shape': ShapefileSource,
        'metatab': MetapackSource,
        'metapack': MetapackSource,
    }

    d.update(custom_proto_map)

    return d

def TYPE_TO_SOURCE_MAP():
    d =  {
        'gs': CsvSource,
        'csv': CsvSource,
        'socrata': CsvSource,
        'metapack': MetapackSource,
        'tsv': TsvSource,
        'fixed': FixedSource,
        'txt': FixedSource,
        'xls': ExcelSource,
        'xlsx': ExcelSource,
        'shape': ShapefileSource,
        'metatab': MetapackSource,
        'ipynb': NotebookSource
    }

    d.update(custom_type_map)

    return d


def get_dflo(spec, syspath):
    import re
    import io
    from zipfile import ZipFile

    if spec.is_archive:

        # Create a DelayedFlo for the file in a ZIP file. We might have to find the file first, though
        def _open(mode='r', encoding=None):
            zf = ZipFile(syspath)

            nl = list(zf.namelist())

            real_name = None

            if spec.target_file:
                # The archive file names can be regular expressions
                real_file_names = list([e for e in nl if re.search(spec.target_file, e)
                                        and not (e.startswith('__') or e.startswith('.'))
                                        ])

                if real_file_names:
                    real_name = real_file_names[0]
                else:
                    raise SourceError("Didn't find target_file '{}' in  '{}' ".format(spec.target_file, syspath))
            else:
                real_file_names = real_files_in_zf

                if real_file_names:
                    real_name = real_file_names[0]
                else:
                    raise SourceError("Can't find target file in '{}' ".format(spec.target_file, syspath))

            if 'b' in mode:
                flo = zf.open(real_name, mode.replace('b', ''))
            else:
                flo = io.TextIOWrapper(zf.open(real_name, mode),
                                       encoding=spec.encoding if spec.encoding else 'utf8')

            return (zf, flo)

        def _close(f):
            f[1].close()
            f[0].close()

        df = DelayedFlo(syspath, _open, lambda m: m[1], _close)

    else:

        def _open(mode='rbU'):
            if 'b' in mode:
                return io.open(syspath, mode)
            else:
                return io.open(syspath, mode,
                               encoding=spec.encoding if spec.encoding else 'utf8')

        def _close(f):
            f.close()

        df = DelayedFlo(syspath, _open, lambda m: m, _close)

    return df


def get_generator(spec, cache_fs, account_accessor=None, clean=False, logger=None, working_dir='', callback=None):
    """Download the container for a source spec and return a DelayedFlo object for opening, closing
      and accessing the container"""

    from os.path import dirname

    d = None

    def download_f():
        return download_and_cache(spec, cache_fs, working_dir=working_dir)

    def try_cls_sig(d, cls):

        try:
            return cls(spec, download_f=download_f, cache=cache_fs)
        except TypeError:
            pass

        if d is None:
            d = download_f()

        try:
            return cls(spec, syspath=d['sys_path'], cache=cache_fs, working_dir=dirname(d['sys_path']))
        except TypeError:
            pass

        return cls(spec, dflo=get_dflo(spec, d['sys_path']), cache=cache_fs, working_dir=dirname(d['sys_path']))

    cls = PROTO_TO_SOURCE_MAP().get(spec.proto)

    # First try the new signature for delayed downloading, which has a
    # download_f argument
    if cls:
        return try_cls_sig(d,cls)

    if spec.resource_format == 'zip' and spec.proto != 'metatab':
        # Details of file are unknown; will have to open it first
        if d is None:
            d = download_f()
        target_file = get_file_from_zip(d, spec)
        spec = spec.update(target_file=target_file)

    cls = TYPE_TO_SOURCE_MAP().get(spec.target_format)

    if cls:
        return try_cls_sig(d,cls)

    raise SourceError("Failed to determine file type for source '{}'; unknown format '{}' "
                .format(spec.url, spec.target_format))


class RowGenerator(SourceSpec):
    """Primary generator object. It's actually a SourceSpec fetches a Source
     then proxies the iterator"""

    def __init__(self, url, name=None, proto=None, resource_format=None,
                 target_file=None, target_segment=None, target_format=None, encoding=None,
                 columns=None,
                 cache=None,
                 working_dir=None,
                 generator_args=None,
                 **kwargs):

        """

        :param url:
        :param cache:
        :param name:
        :param proto:
        :param format:
        :param urlfiletype:
        :param encoding:
        :param file:
        :param segment:
        :param columns:
        :param kwargs: Sucks up other keyword args so dicts can be expanded into the arg list.
        :return:
        """

        self.cache = cache
        self.headers = None
        self.working_dir = working_dir

        self.generator = None

        super(RowGenerator, self).__init__(url, name=name, proto=proto,
                                           resource_format=resource_format,
                                           target_file=target_file,
                                           target_segment=target_segment,
                                           target_format=target_format,
                                           encoding=encoding,
                                           columns=columns,
                                           generator_args=generator_args,
                                           **kwargs)

        self.generator = self.get_generator(self.cache, working_dir=self.working_dir)

    @property
    def path(self):
        return self._url


    @property
    def is_geo(self):
        return isinstance(self.generator, GeoSourceBase)

    def __iter__(self):

        for row in self.generator:

            if not self.headers:
                self.headers = self.generator.headers

            yield row

    def iter_rp(self):
        """Iterate, yielding row proxy objects rather than rows"""

        from .rowproxy import RowProxy

        row_proxy = None

        for row in self.generator:

            if not self.headers:
                self.headers = self.generator.headers
                row_proxy = RowProxy(self.headers)
                continue

            yield row_proxy.set_row(row)

    def __str__(self):
        return "<RowGenerator ({}): {} >".format(type(self.generator), self.url)


class Source(object):
    """Base class for accessors that generate rows from any source

    Subclasses of Source must override at least _get_row_gen method.
    """

    def __init__(self, spec=None, cache=None, working_dir=None):
        from copy import deepcopy

        self.spec = spec

        self.cache = cache

        self.limit = None  # Set externally to limit number of rows produced

    @property
    def headers(self):
        """Return a list of the names of the columns of this file, or None if the header is not defined.

        This should *only* return headers if the headers are unambiguous, such as for database tables,
        or shapefiles. For other files, like CSV and Excel, the header row can not be determined without analysis
        or specification."""

        return None

    @headers.setter
    def headers(self, v):
        raise NotImplementedError

    @property
    def meta(self):
        return {}

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        if self.limit:
            for i, row in enumerate(self._get_row_gen()):

                if self.limit and i > self.limit:
                    break

                yield row
        else:
            for row in self._get_row_gen():
                yield row

        self.finish()

    def start(self):
        pass

    def finish(self):
        pass

    def open(self):
        return self._dflo.open('r')


class SourceFile(Source):
    """Base class for accessors that generate rows from a source file

    Subclasses of SourceFile must override at lease _get_row_gen method.
    """

    def __init__(self, spec, dflo, cache, working_dir=None):
        """

        :param fstor: A File-like object for the file, already opened.
        :return:
        """
        super(SourceFile, self).__init__(spec, cache)

        self._dflo = dflo
        self._headers = None  # Reserved for subclasses that extract headers from data stream
        self._datatypes = None  # If set, an array of the datatypes for each column, derived from the source

    @property
    def path(self):
        return self._dflo.path

    @property
    def children(self):
        """Return the internal files, such as worksheets of an Excel file. """
        return None


class GeneratorSource(Source):
    def __init__(self, spec, generator):
        super(GeneratorSource, self).__init__(spec)

        self.gen = generator

        if six.callable(self.gen):
            self.gen = self.gen()

    def __iter__(self):
        """ Iterate over all of the lines in the generator. """

        self.start()

        for row in self.gen:
            yield row

        self.finish()


class SocrataSource(Source):
    """Iterates a CSV soruce from the JSON produced by Socrata  """

    def __init__(self, spec, dflo, cache, working_dir=None):

        super(SocrataSource, self).__init__(spec, cache)

        self._socrata_meta = None

        self._download_url = spec.url + '/rows.csv'

        self._csv_source = CsvSource(spec, dflo)

    @classmethod
    def download_url(cls, spec):
        return spec.url + '/rows.csv'

    @property
    def _meta(self):
        """Return the Socrata meta data, as a nested dict"""
        import requests

        if not self._socrata_meta:
            r = requests.get(self.spec.url)

            r.raise_for_status()

            self._socrata_meta = r.json()

        return self._socrata_meta

    @property
    def headers(self):
        """Return headers.  """

        return [c['fieldName'] for c in self._meta['columns']]

    datatype_map = {
        'text': 'str',
        'number': 'float',
    }

    @property
    def meta(self):
        """Return metadata """

        return {
            'title': self._meta.get('name'),
            'summary': self._meta.get('description'),
            'columns': [
                {
                    'name': c.get('fieldName'),
                    'position': c.get('position'),
                    'description': c.get('name') + '.' + c.get('description'),

                }
                for c in self._meta.get('columns')
                ]

        }

    def __iter__(self):
        import os

        self.start()

        for i, row in enumerate(self._csv_source):
            # if i == 0:
            #    yield self.headers

            yield row

        self.finish()


class PandasDataframeSource(Source):
    """Iterates a pandas dataframe  """

    def __init__(self, spec, df, cache, working_dir=None):
        super(PandasDataframeSource, self).__init__(spec, cache)

        self._df = df

    def __iter__(self):
        import os

        self.start()

        df = self._df

        index_names = [n if n else "id" for n in df.index.names]

        yield index_names + list(df.columns)
        if len(df.index.names) == 1:
            idx_list = lambda x: [x]
        else:
            idx_list = lambda x: list(x)

        for index, row in df.iterrows():
            yield idx_list(index) + list(row)


        self.finish()


class CsvSource(SourceFile):
    """Generate rows from a CSV source"""

    delimiter = ','

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        import io
        import six

        import csv

        csv.field_size_limit(sys.maxsize) # For: _csv.Error: field larger than field limit (131072)

        if six.PY3:
            import csv
            mode = 'r'

            reader = csv.reader(self._dflo.open(mode), delimiter=self.delimiter)

        else:
            import unicodecsv as csv
            mode = 'rbU'

            reader = csv.reader(self._dflo.open(mode), delimiter=self.delimiter,
                                encoding=self.spec.encoding if self.spec.encoding else 'utf-8')

        self.start()

        i = 0

        try:
            for row in reader:
                yield row
                i += 1
        except UnicodeDecodeError as e:

            raise TextEncodingError(six.text_type(type(e)) + ';' + six.text_type(e) + "; line={}".format(i))
        except TypeError:
            raise
        except csv.Error:
            # The error is that the underlying handle should return strings, not bytes,
            # but always using the TextIOWrapper has other problems, and the error only occurs
            # for CSV files loaded from Zip files.
            raise
            self._dflo.close()

        except Exception as e:
            raise

        self.finish()

        self._dflo.close()


class TsvSource(CsvSource):
    """Generate rows from a TSV (tab separated value) source"""

    delimiter = '\t'


class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def __init__(self, spec, dflo, cache, working_dir=None):
        """

        Args:
            spec (sources.SourceSpec): specification of the source.
            fstor (sources.util.DelayedOpen):

        """
        from .exceptions import SourceError

        super(FixedSource, self).__init__(spec, dflo, cache)

    def make_fw_row_parser(self):

        parts = []

        if not self.spec.columns:
            raise SourceError('Fixed width source must have a schema defined, with column widths.')

        for i, c in enumerate(self.spec.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise SourceError('Fixed width source {} must have start and width values for {} column '
                                  .format(self.spec.name, c.name))

            parts.append('row[{}:{}]'.format(c.start - 1, c.start + c.width - 1))

        code = 'lambda row: [{}]'.format(','.join(parts))

        return eval(code)

    @property
    def headers(self):
        return [c.name if c.name else i for i, c in enumerate(self.spec.columns)]

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        parser = self.make_fw_row_parser()

        for line in self._fstor.open(mode='r', encoding=self.spec.encoding):
            yield [e.strip() for e in parser(line)]

        self.finish()


class ExcelSource(SourceFile):
    """Generate rows from an excel file"""

    @staticmethod
    def srow_to_list(row_num, s):
        """Convert a sheet row to a list"""

        values = []

        try:
            for col in range(s.ncols):
                values.append(s.cell(row_num, col).value)
        except:
            raise

        return values

    def __iter__(self):
        """Iterate over all of the lines in the file"""
        from xlrd import open_workbook

        f = self._dflo.open('rb')

        self.start()

        file_contents = f.read()

        wb = open_workbook(filename=self._dflo.path, file_contents=file_contents)

        try:
            s = wb.sheets()[int(self.spec.target_segment) if self.spec.target_segment else 0]
        except ValueError:  # Segment is the workbook name, not the number
            s = wb.sheet_by_name(self.spec.target_segment)

        for i in range(0, s.nrows):
            row = self.srow_to_list(i, s)
            if i == 0:
                self._headers = row
            yield row

        self.finish()

        self._dflo.close()

    @property
    def children(self):
        from xlrd import open_workbook

        f = self._dflo.open('rb')

        wb = open_workbook(filename=self._dflo.path, file_contents=f.read())

        sheets = wb.sheet_names()

        self._dflo.close()

        return sheets

    @staticmethod
    def make_excel_date_caster(file_name):
        """Make a date caster function that can convert dates from a particular workbook. This is required
        because dates in Excel workbooks are stupid. """

        from xlrd import open_workbook

        wb = open_workbook(file_name)
        datemode = wb.datemode

        def excel_date(v):
            from xlrd import xldate_as_tuple
            import datetime

            try:

                year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
                return datetime.date(year, month, day)
            except ValueError:
                # Could be actually a string, not a float. Because Excel dates are completely broken.
                from dateutil import parser

                try:
                    return parser.parse(v).date()
                except ValueError:
                    return None

        return excel_date


class MetapackSource(SourceFile):
    def __init__(self, spec, dflo, cache, working_dir):
        super(MetapackSource, self).__init__(spec, dflo, cache)

    @property
    def package(self):
        from metatab import open_package
        return open_package(self.spec.resource_url, cache=self.cache)

    @property
    def resource(self):
        return self.package.resource(self.spec.target_segment)

    def __iter__(self):

        for row in self.resource:
            yield row


class ProgramSource(Source):
    """Generate rows from a program. Takes kwargs from the spec to pass into the program. """

    def __init__(self, spec, syspath, cache, working_dir):

        import platform

        if platform.system() == 'Windows':
            raise NotImplementedError("Program sources aren't working on Windows")

        super(ProgramSource, self).__init__(spec, cache)

        assert working_dir

        self.program = normpath(join(working_dir, self.spec.url_parts.path))

        if not exists(self.program):
            raise SourceError("Program '{}' does not exist".format(self.program))

        self.args = dict(list(self.spec.generator_args.items() if self.spec.generator_args else [])+list(self.spec.kwargs.items()))

        self.options = []

        self.properties = {}

        self.env = dict(environ.items())

        # Expand the generator args and kwargs into parameters for the program,
        # which may be command line options, env vars, or a json encoded dict in the PROPERTIES
        # envvar.
        for k, v in self.args.items():
            if k.startswith('--'):
                # Long options
                self.options.append("{} {}".format(k,v))
            elif k.startswith('-'):
                # Short options
                self.options.append("{} {}".format(k, v))
            elif k == k.upper():
                #ENV vars
                self.env[k] = v
            else:
                # Normal properties, passed in as JSON and as an ENV
                self.properties[k] = v

        self.env['PROPERTIES'] = json.dumps(self.properties)


    def start(self):
        pass

    def finish(self):
        pass

    def open(self):
        pass

    def __iter__(self):
        import csv
        import subprocess
        from io import TextIOWrapper
        import json

        # SHould probably give the child process the -u option,  http://stackoverflow.com/a/17701672
        p = subprocess.Popen([self.program] + self.options,
                        stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                        env = self.env)

        p.stdin.write(json.dumps(self.properties).encode('utf-8'))

        try:
            r = csv.reader(TextIOWrapper(p.stdout))
        except AttributeError:
            # For Python 2
            r = csv.reader(p.stdout)

        for row in r:
            yield row


class NotebookSource(Source):
    """Generate rows from an IPython Notebook. """

    def __init__(self, spec,  syspath, cache, working_dir):

        super(NotebookSource, self).__init__(spec, cache)

        self.sys_path = syspath
        if not exists(self.sys_path):
            raise SourceError("Notebook '{}' does not exist".format(self.sys_path))

        self.env = dict(
            (list(self.spec.generator_args.items()) if self.spec.generator_args else [])  +
            (list(self.spec.kwargs.items()) if self.spec.kwargs else [] )
            )

        assert 'METATAB_DOC' in self.env


    def start(self):
        pass

    def finish(self):
        pass

    def open(self):
        pass


    def __iter__(self):

        import pandas as pd

        env = self.execute()

        o = env[self.spec.target_segment]

        if isinstance(o, pd.DataFrame):
            r = PandasDataframeSource(self.spec,o,self.cache)

        else:
            raise Exception("NotebookSource can't handle type: '{}' ".format(type(o)))


        for row in r:
            yield row


    def execute(self):
        """Convert the notebook to a python script and execute it, returning the local context
        as a dict"""

        from nbconvert.exporters import get_exporter

        exporter = get_exporter('python')()

        (script, notebook) = exporter.from_filename(filename=self.sys_path)

        exec(compile(script.replace('# coding: utf-8', ''), 'script', 'exec'), self.env)


        return self.env


class GoogleAuthenticatedSource(SourceFile):
    """Generate rows from a Google spreadsheet source that requires authentication

    To read a GoogleSpreadsheet, you'll need to have an account entry for google_spreadsheets, and the
    spreadsheet must be shared with the client email defined in the credentials.

    Visit http://gspread.readthedocs.org/en/latest/oauth2.html to learn how to generate the credential file, then
    copy the entire contents of the file into the a 'google_spreadsheets' key in the accounts file.

    Them share the google spreadsheet document with the email addressed defined in the 'client_email' entry of
    the credentials.

    """

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for row in self._fstor.get_all_values():
            yield row

        self.finish()


class GooglePublicSource(CsvSource):
    url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'

    @classmethod
    def download_url(cls, spec):
        return cls.url_template.format(key=spec.netloc)


class GeoSourceBase(SourceFile):
    """ Base class for all geo sources. """
    pass


class ShapefileSource(GeoSourceBase):
    """ Accessor for shapefiles (*.shp) with geo data. """

    def __init__(self, spec, syspath, cache, working_dir):
        super(ShapefileSource, self).__init__(spec, None, cache)

        self.syspath = syspath


    def _convert_column(self, shapefile_column):
        """ Converts column from a *.shp file to the column expected by ambry_sources.

        Args:
            shapefile_column (tuple): first element is name, second is type.

        Returns:
            dict: column spec as ambry_sources expects

        Example:
            self._convert_column((u'POSTID', 'str:20')) -> {'name': u'POSTID', 'type': 'str'}

        """
        name, type_ = shapefile_column
        type_ = type_.split(':')[0]
        return {'name': name, 'type': type_}

    def _get_columns(self, shapefile_columns):
        """ Returns columns for the file accessed by accessor.

        Args:
            shapefile_columns (SortedDict): key is column name, value is column type.

        Returns:
            list: list of columns in ambry_sources format

        Example:
            self._get_columns(SortedDict((u'POSTID', 'str:20'))) -> [{'name': u'POSTID', 'type': 'str'}]

        """
        #
        # first column is id and will contain id of the shape.
        columns = [{'name': 'id', 'type': 'int'}]

        # extend with *.shp file columns converted to ambry_sources format.
        columns.extend(list(map(self._convert_column, iter(shapefile_columns.items()))))

        # last column is wkt value.
        columns.append({'name': 'geometry', 'type': 'geometry_type'})
        return columns

    @property
    def headers(self):
        """Return headers. This must be run after iteration, since the value that is returned is
        set in iteration """

        return list(self._headers)

    def __iter__(self):
        """ Returns generator over shapefile rows.

        Note:
            The first column is an id field, taken from the id value of each shape
            The middle values are taken from the property_schema
            The last column is a string named geometry, which has the wkt value, the type is geometry_type.

        """

        # These imports are nere, not at the module level, so the geo
        # support can be an extra

        import fiona
        from fiona.crs import from_epsg, to_string, from_string
        from shapely.geometry import asShape
        from shapely.wkt import dumps
        from zipfile import ZipFile
        from shapely.ops import transform
        import pyproj
        from functools import partial

        layer_index = self.spec.target_segment or 0

        if self.spec.resource_format == 'zip':
            # Find the SHP file. I thought Fiona used to do this itself ...
            shp_file = '/'+next(n for n in ZipFile(self.syspath).namelist() if (n.endswith('.shp') or n.endswith('geojson')))
            vfs = 'zip://{}'.format(self.syspath)
        else:
            shp_file = self.syspath
            vfs = None

        self.start()

        with fiona.open(shp_file, vfs=vfs, layer=layer_index) as source:

            if source.crs.get('init') != 'epsg:4326':
                # Project back to WGS84

                project = partial(pyproj.transform,
                                  pyproj.Proj(source.crs, preserve_units=True),
                                  pyproj.Proj(from_epsg('4326'))
                                  )

            else:
                project = None

            property_schema = source.schema['properties']

            self.spec.columns = [c for c in self._get_columns(property_schema)]
            self._headers = [x['name'] for x in self._get_columns(property_schema)]

            yield self.headers

            for i,s in enumerate(source):

                row_data = s['properties']
                shp = asShape(s['geometry'])

                row = [int(s['id'])]
                for col_name, elem in six.iteritems(row_data):
                    row.append(elem)

                if project:
                    row.append(transform(project, shp))

                else:
                    row.append(shp)

                yield row

        self.finish()


class DataRowGenerator(object):
    """Returns only rows between the start and end lines, inclusive """

    def __init__(self, seq, start=0, end=None, **kwargs):
        """
        An iteratable wrapper that coalesces headers and skips comments

        :param seq: An iterable
        :param start: The start of data row
        :param end: The last row number for data
        :param kwargs: Ignored. Sucks up extra parameters.
        :return:
        """

        self.iter = iter(seq)
        self.start = start
        self.end = end
        self.headers = []  # Set externally

        int(self.start)  # Throw error if it is not an int
        assert self.start > 0

    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i < self.start or (self.end is not None and i > self.end):
                continue

            yield row


class SelectiveRowGenerator(object):
    """Proxies an iterator to remove headers, comments, blank lines from the row stream.
    The header will be emitted first, and comments are avilable from properties """

    def __init__(self, seq, start=0, headers=[], comments=[], end=[], load_headers=True, **kwargs):
        """
        An iteratable wrapper that coalesces headers and skips comments

        :param seq: An iterable
        :param start: The start of data row
        :param headers: An array of row numbers that should be coalesced into the header line, which is yieled first
        :param comments: An array of comment row numbers
        :param end: The last row number for data
        :param kwargs: Ignored. Sucks up extra parameters.
        :return:
        """

        self.iter = iter(seq)
        self.start = start if (start or start is 0) else 1
        self.header_lines = headers if isinstance(headers, (tuple, list)) else [int(e) for e in headers.split(',') if e]
        self.comment_lines = comments
        self.end = end

        self.load_headers = load_headers

        self.headers = []
        self.comments = []

        int(self.start)  # Throw error if it is not an int

    @property
    def coalesce_headers(self):
        """Collects headers that are spread across multiple lines into a single row"""
        from six import text_type
        import re

        if not self.headers:
            return None

        header_lines = [list(hl) for hl in self.headers if bool(hl)]

        if len(header_lines) == 0:
            return []

        if len(header_lines) == 1:
            return header_lines[0]

        # If there are gaps in the values of a line, copy them forward, so there
        # is some value in every position
        for hl in header_lines:
            last = None
            for i in range(len(hl)):
                hli = text_type(hl[i])
                if not hli.strip():
                    hl[i] = last
                else:
                    last = hli

        headers = [' '.join(text_type(col_val).strip() if col_val else '' for col_val in col_set)
                   for col_set in zip(*header_lines)]

        headers = [re.sub(r'\s+', ' ', h.strip()) for h in headers]

        return headers

    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i in self.header_lines:
                if self.load_headers:
                    self.headers.append(row)
            elif i in self.comment_lines:
                self.comments.append(row)
            elif i == self.start:
                break

        if self.headers:
            yield self.coalesce_headers
        else:
            # There is no header, so fake it
            yield ['col' + str(i) for i, _ in enumerate(row)]

        yield row

        for row in self.iter:
            yield row


class DictRowGenerator(object):
    """Constructed on a RowGenerator, returns dicts from the second and subsequent rows, using the
    first row as dict keys. """

    def __init__(self, rg):
        self._rg = rg

    def __iter__(self):
        from six import text_type

        headers = None

        for row in self._rg:
            if not headers:
                headers = [text_type(e).strip() for e in row]
                continue

            yield dict(zip(headers, row))


class MPRSource(Source):
    def __init__(self, spec, datafile, predicate=None, headers=None):
        super(MPRSource, self).__init__(spec)

        self.datafile = datafile

        self.predicate = predicate
        self.return_headers = headers

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        with self.datafile.reader as r:
            for i, row in enumerate(r.select(predicate=self.predicate, headers=self.return_headers)):

                if i == 0:
                    yield row.headers

                yield row.row  # select returns a RowProxy

        self.finish()


class AspwCursorSource(Source):
    """Iterates a ASPW cursor, also extracting the header and type information  """

    def __init__(self, spec, cursor):

        super(AspwCursorSource, self).__init__(spec)

        self._cursor = cursor

        self._datatypes = []

    def __iter__(self):
        import os

        self.start()

        for i, row in enumerate(self._cursor):

            if i == 0:
                self._headers = [e[0] for e in self._cursor.getdescription()]

                yield self._headers

            yield row

        self.finish()


import sys

__all__ = [k for k in sys.modules[__name__].__dict__.keys()
           if not k.startswith('_') and k not in ('sys', 'util', 'petl', 'copy_file_or_flo')]
