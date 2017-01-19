# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""
The SourceSpec defines what kind of a source file to fetch and how to process it.
"""

import hashlib
from rowgenerators.util import parse_url_to_dict, unparse_url_dict
from six import text_type
from .exceptions import SpecError
from .util import parse_url_to_dict, unparse_url_dict

def decompose_url(url, force_archive = None):
    """Decompose and classify a URL, returning the downloadable reference, internal file references, and API
    access URLS. """

    from os.path import splitext


    parts = parse_url_to_dict(url)

    proto = parts['scheme_extension'] if parts.get('scheme_extension') \
            else { 'https': 'http','':'file'}.get(parts['scheme'],parts['scheme'])

    def file_ext(v):
        try:
            return splitext(v)[1][1:]
        except IndexError:
            return None

    is_archive = file_ext(parts['path']) in ('zip',) or force_archive

    file = segment = None

    if parts['fragment']:

        frag_parts = parts['fragment'].split(';')

        # An archive file might have an inner Excel file, and that file can have
        # a segment.
        if is_archive and frag_parts:
            if len(frag_parts) == 2:
                file = frag_parts[0]
                segment = frag_parts[1]
            else:
                file = frag_parts[0]
        elif frag_parts:
            segment = frag_parts[0]

    del parts['fragment']
    del parts['scheme_extension']

    url = unparse_url_dict(parts)

    if proto == 'gs':
        url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'
        download_url = url_template.format(key=parts['netloc']) # netloc is case-sensitive, hostname is forced lower.
        if segment:
            download_url += "?gid={}".format(segment)
    elif proto == 'socrata':
        download_url = url + '/rows.csv'
    else:
        download_url = url

    if is_archive:
        if file:
            format = file_ext(file)
        else:
            file_maybe, ext = splitext(parts['path'])
            format_maybe = file_ext(file_maybe)
            if format_maybe in ('csv','xls','xlsx'):
                format = format_maybe
                file='.*\.'+format
            else:
                format = None

    else:
        format = None

    if not format and proto in ('gs','socrata',):
        format = 'csv'
    elif not format:
        format = file_ext(parts['path'])

    return dict(
        url=url,
        download_url=download_url,
        proto=proto,
        is_archive=is_archive,
        archive_file=file,
        file_segment=segment,
        file_format=format
    )


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
                columns=self.columns
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
                columns=self.columns
            )



    @property
    def file(self):
        return self._file or self.archive_file

    @file.setter
    def file(self,v):
        self._file = v

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
        self._filetype = v

    @property
    def netloc(self):
        """Return the netlocatino part of the URL"""
        p = parse_url_to_dict(self.url)
        return p['netloc']

    def is_archive_url(self):
        return self.urlfiletype in ('zip',)

    def get_generator(self, cache=None):

        if cache is None:
            from fs.opener import fsopendir
            cache = fsopendir('temp://')

        from rowgenerators.fetch import get_generator

        return get_generator(self, cache)

    def url_str(self, file=None, segment=None):
        from .util import parse_url_to_dict, unparse_url_dict

        second_sep = ''

        parts = parse_url_to_dict(self.url)
        del parts['fragment']

        url = unparse_url_dict(parts)

        file = file if file is not None else self.file
        segment = segment if segment is not None else self.segment

        if file is not None or segment is not None:
            url += '#'

        if file is not None:
            url += file
            second_sep = ';'

        if segment is not None:
            url += second_sep
            url += segment

        # The scheme_extension is removed from self.url, so unparse_url_dict won't put it back
        if self.proto != parts['scheme']:
            url = self.proto + '+' + url

        return url

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

    @property
    def dict(self):

        d = dict(url=self.url_str())

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


