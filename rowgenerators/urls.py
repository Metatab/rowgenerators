# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""Functions and classes for processing different kinds of URLs"""

from __future__ import print_function

from os.path import splitext, basename, join, dirname
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




def extract_proto(url):
    parts = parse_url_to_dict(url)

    return parts['scheme_extension'] if parts.get('scheme_extension') \
        else {'https': 'http', '': 'file'}.get(parts['scheme'], parts['scheme'])


def url_is_absolute(ref):

    u = Url(ref)

    if u.scheme in ('http','https'):
        return True



class Url(object):
    """Base class for URL Managers


    url: The input URL
    proto: The extension of the scheme (git+http://, etc), if there is one, otherwise the scheme.

    """

    archive_formats = ['zip']

    def __new__(cls, url, **kwargs):
        return super(Url, cls).__new__(get_handler(url, **kwargs))

    def __init__(self, url, **kwargs):

        assert 'is_archive' not in kwargs

        self.url = reparse_url(url)
        self.parts = self.url_parts(self.url, **kwargs)

        self.scheme = kwargs.get('scheme', self.parts.scheme)
        self.proto = kwargs.get('proto')
        self.resource_url = kwargs.get('resource_url')
        self.resource_file = kwargs.get('resource_file')
        self.resource_format = kwargs.get('resource_format')
        self.target_file = kwargs.get('target_file')
        self.target_format = kwargs.get('target_format')
        self.encoding = kwargs.get('encoding')
        self.target_segment = kwargs.get('target_segment')

        if not self.proto:
            self.proto = extract_proto(self.url)

        self._process_resource_url()
        self._process_fragment()
        self._process_target_file()


    @property
    def is_archive(self):
        return self.resource_format in self.archive_formats

    #property
    def archive_file(self):
        # Return the name of the archive file, if there is one.
        return self.target_file if self.is_archive and self.download_file != self.target_file else None,

    def _process_fragment(self):

        if self.parts.fragment:
            target_file, self.target_segment = self.decompose_fragment(self.parts.fragment, self.is_archive)
        else:
            target_file = self.target_segment = None

        if not self.target_file and target_file:
            self.target_file = target_file

    def _process_resource_url(self):


        self.resource_url = unparse_url_dict(self.parts.__dict__,
                                             scheme=self.parts.scheme if self.parts.scheme else 'file',
                                             fragment=False)

        self.resource_file = basename(self.resource_url)

        if not self.resource_format:
            self.resource_format = file_ext(self.resource_file)


    def _process_target_file(self):


        if not self.target_file:
            self.target_file = basename(self.resource_url)

        if not self.target_format:
            self.target_format = file_ext(self.target_file)

        if not self.target_format:
            self.target_format = self.resource_format


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

        parts['resource_format'] = file_ext(parts['path'])

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

    def component_url(self, s):

        sp = parse_url_to_dict(s)

        if sp['netloc']:
            return s

        url = reparse_url(self.url, path=join(dirname(self.parts.path), sp['path']))
        assert url
        return url

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.resource_url)

    def update(self,**kwargs):
        """Returns a new Url object, possibly with some of the paroperties replaced"""

        o = Url(
            self.rebuild_url(target_file=kwargs.get('target_file', self.target_file),
                             target_segment = kwargs.get('target_segment',self.target_segment)),
            scheme = kwargs.get('scheme', self.scheme),
            proto = kwargs.get('proto',self.proto),
            resource_url = kwargs.get('resource_url',self.resource_url),
            resource_file = kwargs.get('resource_file',self.resource_file),
            resource_format = kwargs.get('resource_format',self.resource_format),
            target_file = kwargs.get('target_file',self.target_file),
            target_format = kwargs.get('target_format',self.target_format),
            encoding = kwargs.get('encoding',self.encoding),
            target_segment = kwargs.get('target_segment',self.target_segment)
        )

        o._process_resource_url()
        o._process_fragment()
        o._process_target_file()

        return o

    def rebuild_url(self, target_file=None, target_segment=None):

        from .util import parse_url_to_dict, unparse_url_dict

        tf = target_file if target_file else self.target_file
        ts = target_segment if (target_segment or target_segment == 0) else self.target_segment

        second_sep = ''

        parts = parse_url_to_dict(self.url)

        f = ''

        if tf:
            f = tf
            second_sep = ';'

        if ts or ts == 0:
            f += second_sep
            f += str(ts)

        parts['fragment'] = f

        return unparse_url_dict(parts)

    @property
    def dict(self):
        from operator import itemgetter

        keys = "url scheme proto resource_url resource_file resource_format target_file target_format "\
               "encoding target_segment"

        return dict( (k, v) for k,v in self.__dict__.items() if k in keys)

    def __deepcopy__(self, o):
        d = self.__dict__.copy()
        del d['url']
        return type(self)(self.url, **d)

    def __copy__(self, o):
        return self.__deepcopy__(o)

class GeneralUrl(Url):
    """Basic URL, with no special handling or protocols"""

    def __init__(self, url, **kwargs):
        super(GeneralUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return True

    def component_url(self, s):
        sp = parse_url_to_dict(s)

        if sp['netloc']:
            return s

        return reparse_url(self.url, path=join(dirname(self.parts.path), sp['path']))


class GoogleProtoCsvUrl(Url):
    """Access a Google spreadheet as a CSV format download"""

    csv_url_template = 'https://docs.google.com/spreadsheets/d/{key}/export?format=csv'

    def __init__(self, url, **kwargs):
        kwargs['resource_format'] = 'csv'
        kwargs['encoding'] = 'utf8'
        kwargs['proto'] = 'gs'
        super(GoogleProtoCsvUrl, self).__init__(url,**kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'gs'

    def _process_resource_url(self):

        self._process_fragment()

        # noinspection PyUnresolvedReferences
        self.resource_url = self.csv_url_template.format(
            key=self.parts.netloc)  # netloc is case-sensitive, hostname is forced lower.

        self.resource_file = self.parts.netloc

        if self.target_segment:
            self.resource_url += "&gid={}".format(self.target_segment)
            self.resource_file += '-' + self.target_segment

        self.resource_file += '.csv'

        if self.resource_format is None:
            self.resource_format = file_ext(self.resource_file)

        self.target_file = self.resource_file  # _process_target() file will use this self.target_file

    def component_url(self, s):

        sp = parse_url_to_dict(s)

        if sp['netloc']:
            return s

        return reparse_url(self.url, fragment=s)

        url = reparse_url(self.resource_url, query="format=csv&gid="+s)
        assert url
        return url


class SocrataUrl(Url):

    def __init__(self, url, **kwargs):
        kwargs['resource_format'] = 'csv'
        kwargs['encoding'] = 'utf8'
        kwargs['proto'] = 'socrata'

        super(SocrataUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'socrata'

    def _process_resource_url(self):

        self.resource_url = unparse_url_dict(self.parts.__dict__,
                                             scheme_extension=False,
                                             fragment=False,
                                             path=join(self.parts.path, 'rows.csv'))

        self.resource_file = basename(self.url) + '.csv'

        if self.resource_format is None:
            self.resource_format = file_ext(self.resource_file)

        self.target_file = self.resource_file  # _process_target() file will use this self.target_file


class CkanUrl(Url):
    def __init__(self, url, **kwargs):
        kwargs['proto'] = 'ckan'
        super(CkanUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 'ckan'


class ZipUrl(Url):
    def __init__(self, url, **kwargs):
        kwargs['resource_format'] = 'zip'
        super(ZipUrl, self).__init__(url, **kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        parts = parse_url_to_dict(url)
        return file_ext(parts['path']) in ('zip',) or kwargs.get('force_archive')

    def _process_fragment(self):

        if self.parts.fragment:
            self.target_file, self.target_segment = self.decompose_fragment(self.parts.fragment, self.is_archive)

        else:
            self.target_file = self.target_segment = None


    def _process_target_file(self):

        # Handles the case of file.csv.zip, etc.
        for ext in ('csv','xls','xlsx'):
            if self.resource_file.endswith('.'+ext+ '.zip'):
                self.target_file = self.resource_file.replace('.zip', '')

        if self.target_file and not self.target_format:
            self.target_format = file_ext(self.target_file)



    def component_url(self, s):

        if url_is_absolute(s):
            return s

        return reparse_url(self.url, fragment=s)


class ExcelUrl(Url):
    resource_format = None  # Must be xls or xlsx

    @classmethod
    def match(cls, url, **kwargs):
        parts = parse_url_to_dict(url)
        return file_ext(parts['path']) in ('xls', 'xlsx')

    def component_url(self, s):

        if url_is_absolute(s):
            return s

        return reparse_url(self.url, fragment=s)



class S3AuthUrl(Url):
    """Access a Google spreadheet as a CSV format download"""

    def __init__(self, url, **kwargs):
        kwargs['proto'] = 's3'
        super(S3AuthUrl, self).__init__(url,**kwargs)

    @classmethod
    def match(cls, url, **kwargs):
        return extract_proto(url) == 's3'

    def _process_resource_url(self):

        url_template = 'https://{bucket}.s3.amazonaws.com/{path}'

        # noinspection PyUnresolvedReferences
        self.resource_url = url_template.format(
            bucket=self.parts.netloc,
            path=self.parts.path.strip('/'))

        self.resource_file = basename(self.resource_url)

        if self.resource_format is None:
            self.resource_format = file_ext(self.resource_file)




url_handlers = [
    CkanUrl,
    SocrataUrl,
    GoogleProtoCsvUrl,
    ZipUrl,
    ExcelUrl,
    S3AuthUrl,
    GeneralUrl
]


def get_handler(url, **kwargs):
    for handler in url_handlers:
        if handler.match(url, **kwargs):
            return handler

    return GeneralUrl
