# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE.txt

"""
The SourceSpec defines what kind of a source file to fetch and how to process it.
"""


from .util import parse_url_to_dict
from rowgenerators.urls import Url
from uuid import uuid4
from copy import deepcopy

class SourceSpec(object):

    # Properties from the internal url that are copied to the SourceSpec
    url_properties = ['scheme','proto','resource_url','resource_file','resource_format','target_file','target_format',
                      'encoding','target_segment' ]
    
    def __init__(self, url, name=None, proto=None, resource_format=None,
                 target_file=None, target_segment=None, target_format=None, encoding=None, 
                 columns=None, **kwargs):
        """

        The ``header_lines`` can be a list of header lines, or one of a few special values:

        * [0]. The header line is the first line in the dataset.
        * False. The header line is not specified, so it should be intuited
        * None or 'none'. There is no header line, and it should not be intuited.

        :param url:
        :param name: An optional name for the source
        :param proto: Either the scheme of the url, or the scheme extension. One of http, https, gs, socrata.
        Forces how the URL is interpreted.
        :param target_format: Forces the file format, which may be either the downloaded resource, or an internal file in a
        ZIP archive. , which is usually taked from the file extension. May be any typical extension string.

        :param file: A reference to an internal file in a Zip archive. May a string, or a regular expression.
        :param segment: A reference to a worksheet in a spreadsheet. May be a string or a number

        :param resource_format: The file format of the object the URL points to, such as a ZIP file, which may
        have internal file of another type.
        :param encoding: The file encoding.

        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:



        The segment may have one or two parameters. If it contains a ';', there are two parameters. The
        first will identify a spreadsheet file in an archive, and the second identifies a worksheet in the
        file.

        """

        if isinstance(url, Url):
            self._url = url
        else:
            self._url = Url(url, proto=proto,
                            resource_format=resource_format.lower() if resource_format else resource_format,
                            target_file=target_file, target_segment=target_segment,
                            target_format=target_format.lower() if target_format else target_format,
                            encoding=encoding)

        self.name = name if name else str(uuid4())
        self.columns = columns
        self.download_time = None  # Set externally

    def __deepcopy__(self, o):
        return type(self)(deepcopy(self._url), name=self.name, columns = self.columns)


    @property
    def url(self):
        return self._url.url

    @property
    def scheme(self):
        return self._url.scheme

    @property
    def proto(self):
        return self._url.proto

    @property
    def resource_url(self):
        return self._url.resource_url

    @property
    def resource_file(self):
        return self._url.resource_file

    @property
    def resource_format(self):
        return self._url.resource_format

    @property
    def target_file(self):
        return self._url.target_file

    @property
    def target_format(self):
        return self._url.target_format

    @property
    def encoding(self):
        return self._url.encoding

    @encoding.setter
    def encoding(self, encoding):
        self._url.encoding = encoding

    @property
    def target_segment(self):
        return self._url.target_segment


    @property
    def is_archive(self):
        return self._url.is_archive

    @property
    def urlfiletype(self):
        raise NotImplementedError()


    def update_format(self):
        raise NotImplementedError()

    @property
    def netloc(self):
        return self._url.parts.netloc

    def is_archive_url(self):
        raise NotImplementedError()

    def get_generator(self, cache=None, working_dir=None):
        from rowgenerators.fetch import get_generator

        return get_generator(self, cache, working_dir=working_dir)

    @property
    def file_name(self):
        raise NotImplementedError()
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
        return "<{} {}>".format(self.__class__.__name__, self.rebuild_url())

    def rebuild_url(self):

        return self._url.rebuild_url()


    @property
    def dict(self):

        d = self._url.dict
        d['name'] = self.name

        return d

    def update(self, **kwargs):

        u = self._url.update(**kwargs)

        return SourceSpec(u, name=self.name, columns=self.columns)


