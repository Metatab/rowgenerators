"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import petl

import six

from .util import copy_file_or_flo
from .exceptions import SourceError
from .sourcespec import SourceSpec


class RowGenerator(SourceSpec):
    """Primary generator object. It's actually a SourceSpec fetches a Source
     then proxies the iterator"""

    def __init__(self, url, cache=None, name=None, urltype=None, filetype=None,
                 urlfiletype=None, encoding=None, file=None,
                 segment=None, columns=None, **kwargs):

        self.cache = cache
        self.headers = None

        super(RowGenerator, self).__init__(url, name, urltype, filetype,
                                           urlfiletype, encoding,
                                           file, segment, columns,
                                           **kwargs)

    @property
    def path(self):
        return self.url

    def __iter__(self):

        self.generator = self.get_generator(self.cache)

        for row in self.generator:
            yield row

        self.headers = self.generator.headers


class Source(object):
    """Base class for accessors that generate rows from any source

    Subclasses of Source must override at least _get_row_gen method.
    """

    def __init__(self, spec=None):
        from copy import deepcopy

        if spec is not None:
            try:
                self.spec = deepcopy(spec)
            except TypeError:
                raise
                pass
        else:
            self.spec = None

        self.limit = None # Set externally to limit number of rows produced

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
            for i,row in enumerate(self._get_row_gen()):

                if self.limit and i > self.limit:
                    break

                yield row
        else:
            for row in self._get_row_gen():
                yield row

        self.finish()

    def _get_row_gen(self):
        """ Returns generator over all rows of the source. """
        raise NotImplementedError('Subclasses of SourceFile must provide a _get_row_gen() method')

    def start(self):
        pass

    def finish(self):
        pass


class SourceFile(Source):
    """Base class for accessors that generate rows from a source file

    Subclasses of SourceFile must override at lease _get_row_gen method.
    """

    def __init__(self, spec, fstor):
        """

        :param fstor: A File-like object for the file, already opened.
        :return:
        """
        super(SourceFile, self).__init__(spec)

        self._fstor = fstor
        self._headers = None  # Reserved for subclasses that extract headers from data stream
        self._datatypes = None # If set, an array of the datatypes for each column, derived from the source

    @property
    def path(self):
        return self._fstor.path

    @property
    def syspath(self):
        return self._fstor.syspath

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
        # TODO (kazbek): Isn't returning self.gen from _get_row_gen method a better choice? Try it.

        self.start()

        for row in self.gen:
            yield row

        self.finish()


class SocrataSource(Source):
    """Iterates a CSV soruce from the JSON produced by Socrata  """

    def __init__(self, spec, fstor):

        super(SocrataSource, self).__init__(spec)

        self._socrata_meta = None

        self._download_url = spec.url+'/rows.csv'

        self._csv_source = CsvSource(spec, fstor)

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

        return [ c['fieldName'] for c in self._meta['columns'] ]

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

            #if i == 0:
            #    yield self.headers

            yield row

        self.finish()


class PandasDataframeSource(Source):
    """Iterates a pandas dataframe  """

    def __init__(self, spec, df):

        super(PandasDataframeSource, self).__init__(spec)

        self._df = df


    def __iter__(self):
        import os

        self.start()

        df = self._df.reset_index()

        yield ['id'] + list(df.columns)


        for index, row in df.iterrows():

            yield [index] + list(row)

        self.finish()


class CsvSource(SourceFile):
    """Generate rows from a CSV source"""

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        from contextlib import closing
        import six

        self.start()

        if six.PY3:
            import csv
            f = self._fstor.open('rtU', encoding=(self.spec.encoding or 'utf8'))
            reader = csv.reader(f)

            with closing(f):

                i = 0
                try:
                    for row in reader:
                        i += 1

                        yield row
                except Exception as e:

                    raise SourceError(str(type(e)) + ';' + e.message + "; line={}".format(i))

        else:
            import unicodecsv as csv

            # What a mess. In the PyFS interface, The 'b' option conflicts with the 'U' open,and
            # readline is hardcoded to use '\n' anyway.
            # BTW, the need for both may result from the file being saved on a mac. If all else fails,
            # try loading it into a spreadsheet format and save with normal line endings.

            # Need to copy the file, since it may be in a Zip file

            import tempfile
            from ambry_sources.util import copy_file_or_flo

            fout = tempfile.NamedTemporaryFile(delete=False)

            with self._fstor.open('rb') as fin:
                copy_file_or_flo(fin, fout)

            fout.close()

            with open(fout.name, 'rbU') as f:

                if self.spec.encoding:
                    reader = csv.reader(f, encoding=self.spec.encoding)
                else:
                    reader = csv.reader(f)

                i = 0
                try:
                    for row in reader:
                        i += 1
                        yield row
                except Exception as e:
                    raise
                    from ambry_sources.sources.exceptions import SourceError
                    raise SourceError(str(type(e)) + ';' + e.message + "; line={}".format(i))

                finally:
                    import os
                    os.remove(fout.name)

        self.finish()


class TsvSource(SourceFile):
    """Generate rows from a TSV (tab separated value) source"""

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for i, row in enumerate(petl.io.csv.fromtsv(self._fstor, self.spec.encoding)):
            yield row

        self.finish()


class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def __init__(self, spec, fstor):
        """

        Args:
            spec (sources.SourceSpec): specification of the source.
            fstor (sources.util.DelayedOpen):

        """
        from .exceptions import SourceError

        super(FixedSource, self).__init__(spec, fstor)


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

            parts.append('row[{}:{}]'  .format(c.start - 1, c.start + c.width - 1))

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

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for i, row in enumerate(self._get_row_gen()):

            if i == 0:
                self._headers = row

            yield row

        self.finish()

    @property
    def _spath(self):
        from fs.errors import NoSysPathError

        try:
            return self._fstor.syspath

        except NoSysPathError:
            # There is no sys path when the file is in a ZipFile, or other non-traditional filesystem.
            sub_file = self._fstor.sub_cache()

            if not sub_file.exists(self.spec.name):

                with self._fstor.open(mode='rb') as f_in, sub_file.open(self.spec.name, mode='wb') as f_out:
                    copy_file_or_flo(f_in, f_out)

            spath = sub_file.getsyspath(self.spec.name)

            return spath

    def _get_row_gen(self):
        from fs.errors import NoSysPathError

        return self.excel_iter(self._spath, self.spec.segment)

    def excel_iter(self, file_name, segment):
        from xlrd import open_workbook

        def srow_to_list(row_num, s):
            """Convert a sheet row to a list"""

            values = []

            try:
                for col in range(s.ncols):
                    values.append(s.cell(row_num, col).value)
            except:
                raise

            return values

        wb = open_workbook(file_name)

        try:
            s = wb.sheets()[int(segment) if segment else 0]
        except ValueError: # Segment is the workbook name, not the number
            s = wb.sheet_by_name(segment)

        for i in range(0, s.nrows):
            row = srow_to_list(i, s)
            yield row

    @property
    def children(self):
        from xlrd import open_workbook

        wb = open_workbook(self._spath)

        return wb.sheet_names()


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

    def __init__(self, spec, fstor):
        super(ShapefileSource, self).__init__(spec, fstor)

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

    def _get_row_gen(self):
        return iter(self)

    @property
    def headers(self):
        """Return headers. This must be run after iteration, since the value that is returned is
        set in iteration """

        return self._headers

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

        from shapely.geometry import shape
        from shapely.wkt import dumps
        from .spec import ColumnSpec

        self.start()

        with fiona.drivers():
            # retrive full path of the zip and convert it to url
            virtual_fs = 'zip://{}'.format(self._fstor._fs.zf.filename)
            layer_index = self.spec.segment or 0
            with fiona.open('/', vfs=virtual_fs, layer=layer_index) as source:
                # geometry_type = source.schema['geometry']
                property_schema = source.schema['properties']
                self.spec.columns = [ColumnSpec(**c) for c in self._get_columns(property_schema)]
                self._headers = [x['name'] for x in self._get_columns(property_schema)]

                for s in source:
                    row_data = s['properties']
                    shp = shape(s['geometry'])
                    wkt = dumps(shp)
                    row = [int(s['id'])]
                    for col_name, elem in six.iteritems(row_data):
                        row.append(elem)

                    row.append(wkt)

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
        self.headers = [] # Set externally

        int(self.start) # Throw error if it is not an int
        assert self.start > 0


    def __iter__(self):

        for i, row in enumerate(self.iter):

            if i < self.start or ( self.end is not None and i > self.end):
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
        self.start = start if start else 1
        self.header_lines = headers if isinstance(headers, (tuple, list)) else [ int(e) for e in headers.split(',') if e]
        self.comment_lines = comments
        self.end = end

        self.load_headers = load_headers

        self.headers = []
        self.comments = []

        int(self.start) # Throw error if it is not an int
        assert self.start > 0


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
            yield [ 'col'+str(i) for i, _ in enumerate(row)]

        yield row

        for row in self.iter:
            yield row

# Ancient Mystery Sources!
# Not actually mysteries, but should be moved back into Ambry

class PartitionSource(SourceFile):
    """Generate rows from a partition. """

    def _get_row_gen(self):
        # TODO: Where is self.bundle definition?
        for row in self.bundle.library.partition(self.spec.url).stream():
            yield row


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
                self._headers = [ e[0] for e in self._cursor.getdescription()]

                yield self._headers

            yield row

        self.finish()

import sys
__all__ = [k for k in sys.modules[__name__].__dict__.keys()
           if not k.startswith('_') and k not in ('sys', 'util','petl', 'copy_file_or_flo')]