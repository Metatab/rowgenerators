# -*- coding: utf-8 -*-
"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import hashlib
from six import text_type, string_types
import os

class SourceSpec(object):

    def __init__(self, url, name=None, segment=None, urltype=None, filetype=None,
                 encoding=None, columns=None,  **kwargs):
        """

        The ``header_lines`` can be a list of header lines, or one of a few special values:

        * [0]. The header line is the first line in the dataset.
        * False. The header line is not specified, so it should be intuited
        * None or 'none'. There is no header line, and it should not be intuited.

        :param url:
        :param segment: A reference to an internal file in a spreadsheet or Zip archive. May be a number,
        a string, or a regular expression.

        :param urltype:
        :param filetype:
        :param encoding:
        :param columns: A list or tuple of ColumnSpec objects, for FixedSource
        :param name: An optional name for the source
        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:
        """

        if 'reftype' in kwargs and not urltype:
            urltype = kwargs['reftype']  # Ambry SourceFile object changed from urltype to reftype.

        def norm(v):

            if v == 0:
                return 0

            if bool(v):
                return v
            else:
                return None

        assert not isinstance(columns, dict)

        try:
            assert not isinstance(columns[0], dict)
        except:
            pass

        self.url = url
        self.urltype=urltype
        self.filetype=filetype
        self.name = name
        self.segment = segment
        self.encoding = encoding
        self.columns = columns

        self.download_time = None  # Set externally

        self.encoding = self.encoding if self.encoding else None

        if not self.name:
            raw_name = '{}#{}'.format(self.url, self.segment)
            if isinstance(raw_name, text_type):
                raw_name = raw_name.encode('utf-8')

            self.name = hashlib.md5(raw_name).hexdigest()

    @property
    def has_rowspec(self):
        """Return True if the spec defines header lines or the data start line"""
        return self._header_lines_specified or self.start_line is not None

    def get_filetype(self, file_path):
        """Determine the format of the source file, by reporting the file extension"""
        from os.path import splitext

        # The filetype is explicitly specified
        if self.filetype:
            return self.filetype.lower()

        root, ext = splitext(file_path)

        return ext[1:].lower()

    def get_urltype(self):
        from os.path import splitext

        if self.urltype:
            return self.urltype

        if self.url and self.url.startswith('gs://'):
            return 'gs'  # Google spreadsheet

        if self.url and self.url.startswith('file://'):
            return 'file'  # Google spreadsheet

        if self.url:

            if '#' in self.url:
                url, frag = self.url.split('#')
            else:
                url = self.url

            root, ext = splitext(url)
            return ext[1:].lower()

        return None

    def get_generator(self, cache=None):

        if cache is None:
            from fs.opener import fsopendir
            cache = fsopendir('temp://')

        from rowgenerators.fetch import get_source

        return get_source(self, cache)

    def __str__(self):
        return str(self.__dict__)


class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
    def __init__(self, fs, path, mode='r', container=None, account_accessor=None):

        self._fs = fs
        self._path = path
        self._mode = mode
        self._container = container
        self._account_accessor = account_accessor

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    @property
    def syspath(self):
        return self._fs.getsyspath(self._path)

    def sub_cache(self):
        """Return a fs directory associated with this file """
        import os.path

        if self._container:
            fs, container_path = self._container

            dir_path = os.path.join(container_path + '_')

            fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return fs.opendir(dir_path)

        else:

            dir_path = os.path.join(self._path+'_')

            self._fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return self._fs.opendir(dir_path)

    @property
    def path(self):
        return self._path

    def __str__(self):

        from fs.errors import NoSysPathError

        try:
            return self.syspath
        except NoSysPathError:
            return "Delayed Open: {}; {} ".format(str(self._fs), str(self._path))

class DelayedDownload(DelayedOpen):
    """An extension of DelayedOpen that also delays downloading the file"""

    def __init__(self, url, fs,  mode='r', logger = None, container=None, account_accessor=None):

        self._url = url
        self._logger = logger
        self._fs = fs
        self._mode = mode
        self._container = container
        self._account_accessor = account_accessor

    def _download(self):
        from ambry_sources.fetch import download

        self._path , _ = download(self._url, self._fs, self._account_accessor, logger=self._logger)

    def open(self, mode=None, encoding=None):
        self._download()
        return super(DelayedDownload, self).open(mode=mode, encoding=encoding)


class RowProxy(object):
    '''
    A dict-like accessor for rows which holds a constant header for the keys. Allows for faster access than
    constructing a dict, and also provides attribute access

    >>> header = list('abcde')
    >>> rp = RowProxy(header)
    >>> for i in range(10):
    >>>     row = [ j for j in range(len(header)]
    >>>     rp.set_row(row)
    >>>     print rp['c']

    '''

    def __init__(self, keys):

        self.__keys = keys
        self.__row = [None] * len(keys)
        self.__pos_map = {e: i for i, e in enumerate(keys)}
        self.__initialized = True

    @property
    def row(self):
        return object.__getattribute__(self, '_RowProxy__row')

    def set_row(self, v):
        object.__setattr__(self, '_RowProxy__row', v)
        return self

    @property
    def headers(self):
        return self.__getattribute__('_RowProxy__keys')

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.__row[key] = value
        else:
            self.__row[self.__pos_map[key]] = value

    def __getitem__(self, key):

            if isinstance(key, int):
                try:
                    return self.__row[key]
                except IndexError:
                    raise KeyError("Failed to get value for integer key '{}' ".format(key))
            else:
                try:
                    return self.__row[self.__pos_map[key]]
                except IndexError:
                    raise IndexError("Failed to get value for non-int key '{}', resolved to position {} "
                                     .format(key, self.__pos_map[key]))
                except KeyError:
                    raise KeyError("Failed to get value for non-int key '{}' ".format(key))

    def __setattr__(self, key, value):

        if '_RowProxy__initialized' not in self.__dict__:
            return object.__setattr__(self, key, value)

        else:
            self.__row[self.__pos_map[key]] = value

    def __getattr__(self, key):
        try:
            return self.__row[self.__pos_map[key]]
        except KeyError:
            raise KeyError("Failed to find key '{}'; has {}".format(key, self.__keys))

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.__keys)

    def __len__(self):
        return len(self.__keys)

    @property
    def dict(self):
        return dict(zip(self.__keys, self.__row))

    def copy(self):
        return type(self)(self.__keys).set_row(list(self.row))

    def keys(self):
        return self.__keys

    def values(self):
        return self.__row

    def items(self):
        return zip(self.__keys, self.__row)

    # The final two methods aren't required, but nice for demo purposes:
    def __str__(self):
        """ Returns simple dict representation of the mapping. """
        return str(self.dict)

    def __repr__(self):
        return self.dict.__repr__()


class GeoRowProxy(RowProxy):

    @property
    def __geo_interface__(self):
        from shapely.wkt import loads

        g = loads(self.geometry)
        gi = g.__geo_interface__

        d = dict(self)
        del d['geometry']

        gi['properties'] = d

        return gi



def copy_file_or_flo(input_, output, buffer_size=64 * 1024, cb=None):
    """ Copy a file name or file-like-object to another file name or file-like object"""

    assert bool(input_)
    assert bool(output)

    input_opened = False
    output_opened = False

    try:
        if isinstance(input_, string_types):

            if not os.path.isdir(os.path.dirname(input_)):
                os.makedirs(os.path.dirname(input_))

            input_ = open(input_, 'r')
            input_opened = True

        if isinstance(output, string_types):

            if not os.path.isdir(os.path.dirname(output)):
                os.makedirs(os.path.dirname(output))

            output = open(output, 'wb')
            output_opened = True

        # shutil.copyfileobj(input_,  output, buffer_size)

        def copyfileobj(fsrc, fdst, length=buffer_size):
            cumulative = 0
            while True:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if cb:
                    cumulative += len(buf)
                    cb(len(buf), cumulative)

        copyfileobj(input_, output)

    finally:
        if input_opened:
            input_.close()

        if output_opened:
            output.close()
