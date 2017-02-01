# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""
The SourceSpec defines what kind of a source file to fetch and how to process it.
"""

import hashlib
from rowgenerators.urls import decompose_url, file_ext
from six import text_type
from .exceptions import SpecError
from .util import parse_url_to_dict


class SourceSpec(object):
    def __init__(self, url, name=None, urltype=None, filetype=None, format=None, urlfiletype=None,
                 encoding=None, file=None, segment=None, columns=None, **kwargs):
        """

        The ``header_lines`` can be a list of header lines, or one of a few special values:

        * [0]. The header line is the first line in the dataset.
        * False. The header line is not specified, so it should be intuited
        * None or 'none'. There is no header line, and it should not be intuited.

        :param name: An optional name for the source
        :param url:
        :param file: A reference to an internal file in a Zip archive. May a string, or a regular expression.
        :param sheet: A reference to a worksheet in a spreadsheet. May be a string or a number
        :param urltype: One of http, https, gs, socrata. Forces how the URL is interpreted. Only 'socrata' is really
        needed
        :param format: Forces the file type, which is usually taked from the file extension. May be any
        typical extension string.
        :param urlfiletype: Like filetype, but for when the URL refers to a zip archive.
        :param encoding: The file encoding.
        :param columns: A list or tuple of ColumnSpec objects, for FixedSource
        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:

        `format` is the deprecated version of format

        The segment may have one or two parameters. If it contains a ';', there are two parameters. The
        first will identify a spreadsheet file in an archive, and the second identifies a worksheet in the
        file.

        """

        assert not isinstance(columns, dict)

        try:
            assert not isinstance(columns[0], dict)
        except:
            pass

        self.name = name
        self.columns = columns
        self.encoding = encoding if encoding else None
        self._urltype = urltype
        self._internalurltype = False # Set if the _urltype is set from the url
        self._urlfiletype = urlfiletype
        self._format =  format or filetype
        self._file = file
        self._segment = segment

        self.download_time = None  # Set externally

        if url:
            self.__dict__.update(decompose_url(url, force_archive=self._urlfiletype in ('zip',)))
        else:
            self.url = None
            self.download_url = None,
            self.proto = None,
            self.is_archive = None,
            self.archive_file = None,
            self.file_segment = None,
            self.file_format = None

        if not self.name:
            raw_name = '{}#{}{}'.format(self.url,
                                        (self.file if self.file else ''),
                                        (self.segment if self.segment else ''))
            if isinstance(raw_name, text_type):
                raw_name = raw_name.encode('utf-8')

            self.name = hashlib.md5(raw_name).hexdigest()



    def __deepcopy__(self, o):

        try:
            return self.__class__(
                url=self.url,
                name=self.name,
                file=self.file,
                segment=self.segment,
                urltype=self._urltype,
                urlfiletype=self._urlfiletype,
                format=self.format,
                encoding=self.encoding,
                columns=self.columns,
                proto=self.proto
            )
        except SpecError as e:
            # Guess that its a conflict of the file or segment param with the url

            return self.__class__(
                url=self.url,
                name=self.name,
                file=self.file,
                urltype=self._urltype,
                urlfiletype=self._urlfiletype,
                format=self.format,
                encoding=self.encoding,
                columns=self.columns,
                proto=self.proto
            )



    @property
    def file(self):
        return self._file or self.archive_file

    @file.setter
    def file(self,v):
        self._file = v
        self.update_format()

    @property
    def segment(self):
        return self._segment or self.file_segment

    @segment.setter
    def segment(self,v):
        self._segment = v

    @property
    def urltype(self):
        return self.proto

    @property
    def urlfiletype(self):
        from os.path import splitext

        if self._urlfiletype:
            return self._urlfiletype

        parts = parse_url_to_dict(self.url)

        root, ext = splitext(parts['path'])

        return ext[1:].lower()

    @property
    def format(self):

        if self._format:
            return self._format

        else:
            return self.file_format


    @format.setter
    def format(self, v):
        self._format = v

    def update_format(self):
        self.format = file_ext(self.file)

    @property
    def netloc(self):
        """Return the netlocatino part of the URL"""
        p = parse_url_to_dict(self.url)
        return p['netloc']

    def is_archive_url(self):
        return self.urlfiletype in ('zip',)

    def get_generator(self, cache=None):
        from rowgenerators.fetch import get_generator

        return get_generator(self, cache)

    @property
    def file_name(self):
        from os.path import basename, splitext, sep
        from six.moves.urllib.parse import unquote
        import re

        url = self.url
        second_sep = ''

        parts = parse_url_to_dict(self.url)
        path, ext = splitext(basename(parts['path']))

        path = unquote(path)

        file = self.file
        segment =  self.segment

        if file is not None or segment is not None:
            path += '-'

        if file is not None:
            file = file.replace(sep, '-')
            path += file
            second_sep = '-'

        if segment is not None:
            path += second_sep
            path += segment

        return re.sub(r'[^\w-]','-',path)

    def __str__(self):
        return str(self.__dict__)

    def rebuild_url(self):
        from .util import parse_url_to_dict, unparse_url_dict

        second_sep = ''

        parts = parse_url_to_dict(self.url)
        del parts['fragment']

        url = unparse_url_dict(parts)

        if self.file is not None or self.segment is not None:
            url += '#'

        if self.file is not None:
            url += self.file
            second_sep = ';'

        if self.segment is not None:
            url += second_sep
            url += self.segment

        return url


    @property
    def dict(self):

        d = dict(url=self.url)

        if self._urltype and not self._internalurltype:
            d['urltype'] = self._urltype

        if self._urlfiletype:
            d['urlfiletype'] = self._urlfiletype

        if self.file:
            from os.path import splitext
            root, ext = splitext(self.file_name)
            file_filetype = ext[1:].lower()
        else:
            file_filetype = None

        if self.format:
            d['format'] = self.format

        if self.encoding:
            d['encoding'] = self.encoding

        return d


