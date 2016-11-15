# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""
The SourceSpec defines what kind of a source file to fetch and how to process it.
"""

from .exceptions import SpecError
from six import text_type
import hashlib
from util import parse_url_to_dict

class SourceSpec(object):

    def __init__(self, url, name=None, file=None, segment=None, urltype=None, filetype=None,
                 encoding=None, columns=None, **kwargs):
        """

        The ``header_lines`` can be a list of header lines, or one of a few special values:

        * [0]. The header line is the first line in the dataset.
        * False. The header line is not specified, so it should be intuited
        * None or 'none'. There is no header line, and it should not be intuited.

        :param name: An optional name for the source
        :param url:
        :param file: A reference to an internal file in a Zip archive. May a string, or a regular expression.
        :param sheet: A reference to a worksheet in a spreadsheet. May be a string or a number
        :param urltype:
        :param filetype:
        :param encoding:
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
        self.urltype=urltype
        self.filetype=filetype
        self.encoding = encoding
        self.columns = columns
        self.file = file
        self.segment = segment

        self.download_time = None  # Set externally

        self.encoding = self.encoding if self.encoding else None

        if not self.name:
            raw_name = '{}#{}{}'.format(self.url,
                                      (self.file if self.file else ''),
                                      (self.segment if self.segment else ''))
            if isinstance(raw_name, text_type):
                raw_name = raw_name.encode('utf-8')

            self.name = hashlib.md5(raw_name).hexdigest()

        if url:
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

    def is_archive_url(self):
        return self.get_urltype() in ('zip',)

    def get_generator(self, cache=None):

        if cache is None:
            from fs.opener import fsopendir
            cache = fsopendir('temp://')

        from rowgenerators.fetch import get_source

        return get_source(self, cache)

    def __str__(self):
        return str(self.__dict__)

