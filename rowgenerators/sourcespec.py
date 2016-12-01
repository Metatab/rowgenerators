# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""
The SourceSpec defines what kind of a source file to fetch and how to process it.
"""

from .exceptions import SpecError
from six import text_type
import hashlib
from util import parse_url_to_dict, unparse_url_dict


class SourceSpec(object):
    def __init__(self, url, name=None, urltype=None, filetype=None, urlfiletype=None,
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
        :param filetype: Forces the file type, which is usually taked from the file extension. May be any
        typical extension string.
        :param urlfiletype: Like filetype, but for when the URL refers to a zip archive.
        :param encoding: The file encoding.
        :param columns: A list or tuple of ColumnSpec objects, for FixedSource
        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:

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
        self.url = url
        self._urltype = urltype
        self._internalurltype = False # Set if the _urltype is set from the url
        self._urlfiletype = urlfiletype
        self._filetype = filetype
        self.encoding = encoding if encoding else None
        self.columns = columns
        self.file = file
        self.segment = segment

        self.download_time = None  # Set externally

        if not self.name:
            raw_name = '{}#{}{}'.format(self.url,
                                        (self.file if self.file else ''),
                                        (self.segment if self.segment else ''))
            if isinstance(raw_name, text_type):
                raw_name = raw_name.encode('utf-8')

            self.name = hashlib.md5(raw_name).hexdigest()

        if self.url:
            parts = parse_url_to_dict(self.url)

            if parts['fragment']:

                if self.file:
                    raise SpecError("'file' specification conflicts with fragment in URL")

                if self.segment:
                    raise SpecError("'sheet' specification conflicts with fragment in URL")

                frag_parts = parts['fragment'].split(';')

                if self.is_archive_url() and frag_parts:
                    if len(frag_parts) == 2:
                        self.file = frag_parts[0]
                        self.segment = frag_parts[1]
                    else:
                        self.file = frag_parts[0]
                elif frag_parts:
                    self.segment = frag_parts[0]

            if parts['scheme_extension']:
                self._urltype = parts['scheme_extension']
                self._internalurltype = True

            del parts['fragment']
            del parts['scheme_extension']

            self.url = unparse_url_dict(parts)


    def __deepcopy__(self, o):

        try:
            return self.__class__(
                url=self.url,
                name=self.name,
                file=self.file,
                segment=self.segment,
                urltype=self._urltype,
                urlfiletype=self._urlfiletype,
                filetype=self._filetype,
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
                filetype=self._filetype,
                encoding=self.encoding,
                columns=self.columns
            )

    def get_filetype(self, file_path):
        """Determine the format of the source file, by reporting the file extension"""
        from os.path import splitext

        # The filetype is explicitly specified
        if self._filetype:
            return self._filetype.lower()

        root, ext = splitext(file_path)

        return ext[1:].lower()

    @property
    def urltype(self):

        if self._urltype:
            return self._urltype

        if not self.url:
            return None

        parts = parse_url_to_dict(self.url)

        if parts['scheme'] in ('http', 'gs', 'socrata', 'file'):
            return parts['scheme']

        if self.url.startswith('https'):
            return 'http'

        return 'file'

    @property
    def urlfiletype(self):
        from os.path import splitext

        if self._urlfiletype:
            return self._urlfiletype

        parts = parse_url_to_dict(self.url)

        root, ext = splitext(parts['path'])

        return ext[1:].lower()

    @property
    def filetype(self):

        if self._filetype:
            return self._filetype

        elif self.file:
            return self.get_filetype(self.file)

        else:
            return self.urlfiletype

    def is_archive_url(self):
        return self.urlfiletype in ('zip',)

    def get_generator(self, cache=None):

        if cache is None:
            from fs.opener import fsopendir
            cache = fsopendir('temp://')

        from rowgenerators.fetch import get_source

        return get_source(self, cache)

    def url_str(self, file=None, segment=None):
        from .util import parse_url_to_dict, unparse_url_dict

        second_sep = ''

        parts = parse_url_to_dict(self.url)
        del parts['fragment']

        if self.urltype in ('socrata'):
            parts['scheme'] = parts['scheme']+'+'+self.urltype

        url  = unparse_url_dict(parts)

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

        return url

    @property
    def file_name(self):
        from os.path import basename, splitext, sep
        url = self.url
        second_sep = ''

        parts = parse_url_to_dict(self.url)
        path, ext = splitext(basename(parts['path']))

        path = path.replace(sep, '-')

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

        return path

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

        if self._filetype and self._filetype != file_filetype:
            d['filetype'] = self._filetype

        if self.encoding:
            d['encoding'] = self.encoding

        return d
