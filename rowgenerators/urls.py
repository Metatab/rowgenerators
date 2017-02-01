# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""Functions and classes for processing different kinds of URLs"""

from __future__ import print_function

from os.path import splitext, basename, join
from rowgenerators.util import parse_url_to_dict, unparse_url_dict, reparse_url


def file_ext(v):
    """Split of the extension of a filename, without throwing an exception of there is no extension. Does not
    return the leading '.'
    :param v: """

    try:
        v = splitext(v)[1][1:]
        return v if v else None
    except IndexError:
        return None


# DEPRECATED. Use Url() instead.
def decompose_url(url, force_archive=None):
    """Decompose and classify a URL, returning the downloadable reference, internal file references, and API
    access URLS.
    :param force_archive:
    :param url: """

    uo = Url(url, force_archive=force_archive)

    return dict(
        url=url,
        download_url=uo.download_url,
        proto=uo.proto,
        is_archive=uo.is_archive,
        archive_file=uo.target_file if uo.is_archive and uo.download_file != uo.target_file else None,
        target_file=uo.target_file,
        file_segment=uo.file_segment,
        file_format=uo.target_format
    )


def extract_proto(url):
    parts = parse_url_to_dict(url)

    return parts['scheme_extension'] if parts.get('scheme_extension') \
        else {'https': 'http', '': 'file'}.get(parts['scheme'], parts['scheme'])


class Url(object):
    """Base class for URL Managers"""

    def __new__(cls, url, **kwargs):
        return super(Url, cls).__new__(get_handler(url, **kwargs))

    def __init__(self, url, **kwargs):

        self.url = reparse_url(url)
        self.parts = self.url_parts(self.url, **kwargs)

        self.proto = kwargs.get('proto')
        self.is_archive = kwargs.get('is_archive')
        self.download_url = kwargs.get('download_url')
        self.download_file = kwargs.get('download_file')
        self.download_format = kwargs.get('download_format')
        self.target_file = kwargs.get('target_file')
        self.target_format = kwargs.get('target_format')
        self.encoding = kwargs.get('encoding')
        self.file_segment = kwargs.get('file_segment')

        if not self.proto:
            self.proto = extract_proto(self.url)

        self._process_fragment()
        self._process_download_url()
        self._process_target_file()

    def _process_fragment(self):

        if self.parts.fragment:
            self.target_file, self.file_segment = self.decompose_fragment(self.parts.fragment, self.is_archive)
        else:
            self.target_file = self.file_segment = None

    def _process_download_url(self):
        self.download_url = unparse_url_dict(self.parts.__dict__,
                                             scheme=self.parts.scheme if self.parts.scheme else 'file',
                                             fragment=False)

        self.download_file = basename(self.download_url)

        if self.download_format is None:
            self.download_format = file_ext(self.download_file)

    def _process_target_file(self):

        if not self.target_file:
            self.target_file = basename(self.download_url)

        if self.target_format is None:
            self.target_format = file_ext(self.target_file)

        if not self.target_format:
            self.target_format = self.download_format

        assert self.target_format, self.url

    @classmethod
    def decompose_fragment(cls, frag, is_archive):

        # noinspection PyUnresolvedReferences
        from six.moves.urllib.parse import unquote_plus

        frag_parts = unquote_plus(frag).split(';')

        file = segment = None

        # An archive file might have an inner Excel file, and that file can have
        # a segment.

        if is_archive and frag_parts:

            if len(frag_parts) == 2:
                file = frag_parts[0]
                segment = frag_parts[1]

            else:
                file = frag_parts[0]
        elif frag_parts:
            # If it isn't an archive, then the only possibility is a spreadsheet with an
            # inner segment
            segment = frag_parts[0]

        return file, segment

    @classmethod
    def url_parts(cls, url, **kwargs):
        from .util import Bunch
        """Return an object of url parts, possibly with updates from the kwargs"""
        parts = parse_url_to_dict(url)

        parts['download_format'] = file_ext(parts['path'])

        parts.update(kwargs)

        return Bunch(parts)

    @classmethod
    def match(cls, url, **kwargs):
        """Return True if this handler can handle the input URL"""
        raise NotImplementedError

    @property
    def generator(self):
        """Return a suitable generator for this url"""
        raise NotImplementedError

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.download_url)


class GeneralUrl(Url):
    """Basic URL, with no special handling or protocols"""

    def __init__(self, url, **kwargs):
        super(GeneralUrl, self).__init__(url)

    @classmethod
    def match(cls, url, **kwargs):
        return True


class GoogleProtoCsvUrl(Url):
    """Access a Google spreadheet as a CSV format download"""

    def __init__(self, url, **kwargs):

        super(GoogleProtoCsvUrl, self).__init__(url,
                                                download_format='csv',
                                                encoding='utf8',
                                                proto='gs')

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'gs'

    def _process_download_url(self):

        url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'

        # noinspection PyUnresolvedReferences
        self.download_url = url_template.format(
            key=self.parts.netloc)  # netloc is case-sensitive, hostname is forced lower.

        self.download_file = self.parts.netloc

        if self.file_segment:
            self.download_url += "&gid={}".format(self.file_segment)
            self.download_file += '-' + self.file_segment

        self.download_file += '.csv'

        if self.download_format is None:
            self.download_format = file_ext(self.download_file)

        self.target_file = self.download_file  # _process_target() file will use this self.target_file


class SocrataUrl(Url):
    def __init__(self, url, **kwargs):
        super(SocrataUrl, self).__init__(url,
                                         download_format='csv',
                                         encoding='utf8',
                                         proto='socrata')

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'socrata'

    def _process_download_url(self):
        self.download_url = unparse_url_dict(self.parts.__dict__,
                                             scheme_extension=False,
                                             fragment=False,
                                             path=join(self.parts.path, 'rows.csv'))

        self.download_file = basename(self.url) + '.csv'

        if self.download_format is None:
            self.download_format = file_ext(self.download_file)

        self.target_file = self.download_file  # _process_target() file will use this self.target_file


class CkanUrl(Url):
    def __init__(self, url, **kwargs):
        super(CkanUrl, self).__init__(url, proto='ckan')

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'ckan'


class ZipUrl(Url):
    def __init__(self, url, **kwargs):
        super(ZipUrl, self).__init__(url,
                                     download_format='zip',
                                     is_archive=True)

    @classmethod
    def match(cls, url, **kwargs):
        parts = parse_url_to_dict(url)
        return file_ext(parts['path']) in ('zip',) or kwargs.get('force_archive')

    def _process_fragment(self):

        if self.parts.fragment:
            self.target_file, self.file_segment = self.decompose_fragment(self.parts.fragment, self.is_archive)

        else:
            self.target_file = self.file_segment = None

    def _process_target_file(self):

        for ext in ('csv','xls','xlsx'):
            if self.download_file.endswith('.'+ext+'.zip'):
                self.target_file = self.download_file.replace('.zip','')

        if not self.target_file:
            self.target_file = basename(self.download_url)

        if self.target_format is None:
            self.target_format = file_ext(self.target_file)


class ExcelUrl(Url):
    download_format = None  # Must be xls or xlsx

    @classmethod
    def match(cls, url, **kwargs):
        parts = parse_url_to_dict(url)
        return file_ext(parts['path']) in ('xls', 'xlsx')


url_handlers = [
    CkanUrl,
    SocrataUrl,
    GoogleProtoCsvUrl,
    ZipUrl,
    ExcelUrl,
    GeneralUrl
]


def get_handler(url, **kwargs):
    for handler in url_handlers:
        if handler.match(url, **kwargs):
            return handler

    return GeneralUrl
