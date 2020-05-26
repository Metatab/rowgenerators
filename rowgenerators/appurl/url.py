# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from os.path import basename
from urllib.parse import unquote

from .util import file_ext, parse_url_to_dict, unparse_url_dict

def match_url_classes(u_str, **kwargs):
    """
    Return the classes for which the url matches an entry_point specification, sorted by priority

    :param u_str: Url string
    :param kwargs: arguments passed to Url constructor
    :return:
    """

    from pkg_resources import iter_entry_points

    u = Url(str(u_str), downloader=None, **kwargs)

    try:
        classes = []

        for ep in iter_entry_points(group='appurl.urls'):
            if u._match_entry_point(ep.name):
                classes.append(ep.load())

        classes = sorted(classes, key=lambda cls: cls.match_priority)

    except ModuleNotFoundError as e:
        raise ModuleNotFoundError("Failed to find module for url string '{}', entrypoint: "
                                  .format(u_str, e))

    return classes

default_downloader = None

def parse_app_url(u_str, downloader='default', **kwargs):
    """
    Parse a URL string and return a Url object, with the class based on the highest priority
    entry point that matches the Url and which of the entry point classes pass the match() test.

    :param u_str: Url string
    :param downloader: Downloader object to use for downloading objects.
    :param kwargs: Args passed to the Url constructor.
    :return:
    """
    from rowgenerators.appurl.web.download import Downloader
    from rowgenerators.exceptions import AppUrlError

    if not u_str:
        return None

    if isinstance(u_str, Url):
        return u_str

    if not isinstance(u_str, str):
        raise AppUrlError("Input isn't a string nor Url")

    if downloader == 'default':
        global default_downloader
        if default_downloader is None:
            default_downloader = Downloader.get_instance()

        downloader = default_downloader

    classes = match_url_classes(u_str, **kwargs)

    u = Url(str(u_str), downloader=None, **kwargs)

    for cls in classes:
        if cls._match(u):
            return cls(str(u_str) if u_str else None, downloader=downloader, **kwargs)


class UrlPartsProp(object):
    """Property descriptor for reading and writting to the _parts dict
    in UrlParts"""
    def __init__(self, name):
        self.name = name

    def __get__(self, obj, objtype):
        return obj._parts.get(self.name)

    def __set__(self, obj, value):
        if value is None and self.name in obj._parts:
            del obj._parts[self.name]
        else:
            obj._parts[self.name] = value

    def __delete__(self, obj):
       del obj._parts[self.name]

class UrlParts(object):
    """Container class for handling property accessors"""

    _url_parts = ['proto', 'scheme_extension', 'scheme',
                 'netloc', 'hostname',
                 'username', 'password', 'port',
                 'path', 'query', 'fragment', 'fragment_query']

    _app_parts = ['resource_file', 'resource_format',
                 'target_file', 'target_format', 'target_segment']

    _fragment_query_parts = ['start','end','headers','encoding',
                             'resource_file','resource_format','target_format']

    _fragment_segments_parts = ['target_file','target_segment']

    _all_parts = set(_url_parts+_app_parts + _fragment_query_parts + _fragment_segments_parts )

    # Add extra fragment parts here.
    _extra_fragement_props = []

    def __init__(self, url, **kwargs):

        self._url = url
        self._kwargs = kwargs

        if self._url:
            self._parts = parse_url_to_dict(self._url)
        else:
            self._parts = {}

        self._convert_fragment()
        self._convert_fragment_query()

        self._parts.update(kwargs)

    def _convert_fragment(self):

        if 'fragment' in self._parts and isinstance(self._parts['fragment'], (list, tuple)):
            if len(self._parts['fragment']) == 1:
                self._parts['target_file'] = self._parts['fragment'][0]
            elif len(self._parts['fragment']) == 2:
                self._parts['target_file'], self._parts['target_segment'] = self._parts['fragment']

            del self._parts['fragment']

    def _convert_fragment_query(self):

        if isinstance(self._parts.get('fragment_query'), dict):

            for k, v in list(self._parts['fragment_query'].items()):
                if k in self._fragment_query_parts:
                    self._parts[k] = self._parts['fragment_query'][k]
                    del self._parts['fragment_query'][k]

    scheme = UrlPartsProp('scheme')
    scheme_extension = UrlPartsProp('scheme_extension')
    netloc = UrlPartsProp('netloc')
    hostname = UrlPartsProp('hostname')
    username = UrlPartsProp('username')
    password = UrlPartsProp('password')
    port = UrlPartsProp('port')
    path = UrlPartsProp('path')
    query = UrlPartsProp('query')
    target_segment = UrlPartsProp('target_segment')
    start = UrlPartsProp('start')
    end = UrlPartsProp('end')
    headers = UrlPartsProp('headers')
    encoding = UrlPartsProp('encoding')

    fragment_query = UrlPartsProp('fragment_query')

    @property
    def proto(self):
        return self._parts.get('proto') or \
               self._parts['scheme_extension'] or \
               {'https': 'http', '': 'file'}.get(self._parts['scheme']) or \
               self._parts['scheme']

    @proto.setter
    def proto(self,v):
        self._parts['proto'] = v

    @property
    def target_format(self):
        from .util import file_ext

        target_format = self._parts.get('target_format')

        if not target_format and self.target_file:
            target_format = file_ext(self.target_file)

        if not target_format:
            target_format = self.resource_format

        # handle URLS that end with package names, like:
        # 'example.com-example_data_package-2017-us-1'
        if target_format and len(target_format) > 8:
            target_format = None

        return target_format

    @target_format.setter
    def target_format(self, v):
        self._parts['target_format'] = v



    def clear_fragment(self):
        """
        Return a copy of the URL with no fragment components

        :return: A cloned URl object, with the fragment and fragment queries cleared.
        """

        c = self.clone()
        c._parts['target_file'] = None
        c._parts['target_segment'] = None

        return c

    #
    # Property accessors
    #

    def set_fragment(self, f):
        """Return a clone with the fragment set"""
        raise NotImplementedError()

    @property
    def resource_file(self):
        if self.path:
            return basename(self.path)
        else:
            return None

    @property
    def resource_format(self):
        return self._parts.get('resource_format') or file_ext(self.resource_file)

    @resource_format.setter
    def resource_format(self, v):
        self._parts['resource_format'] = v

    @property
    def target_file(self):

        return self._parts.get('target_file') or self.resource_file

    @target_file.setter
    def target_file(self, v):
        self._parts['target_file'] = v


    def set_target_file(self, v):
        """Return a clone with a target_file set"""
        u = self.clone()
        u.target_file = v
        return u

    def set_target_segment(self, v):
        """Return a clone with a target_file set"""
        u = self.clone()
        u.target_segment = v
        return u

    @property
    def dict(self):
        """
        Returns a dictionary of the object components.

        :return: a dict.
        """

        d = dict(self._parts.items())

        d['scheme_extension'] = self._parts.get('proto') or d.get('scheme_extension')

        for k, v in list(d.items()):
            if k in (self._fragment_query_parts + self._fragment_segments_parts):
                if not v:
                    del d[k]

        d['fragment'] = [
            self._parts.get('target_file'),
            self._parts.get('target_segment')
        ]

        for k in self._fragment_query_parts:
            if k in d:
                d['fragment_query'][k] = d[k]
                del d[k]

        return d

    @property
    def frag_dict(self):
        d = {}
        for k in self._fragment_segments_parts + self._fragment_query_parts:
            d[k] = self._parts.get(k)

        return d

    def __str__(self):

        return unparse_url_dict(self.dict)




class Url(UrlParts):
    """Base class for Application URLs .

    After construction, a Url object has a set of properties and attributes for access
    the parts of the URL, and method for manipulating it. The attributes and properties
    include the typical properties of a parsed URL, plus properties that are derives from the
    typical parts, and a few extra components that can be part of the fragment query.

    The typical parts are:

    - ``scheme``
    - ``scheme_extension``
    - ``netloc``
    - ``hostname``
    - ``path``
    - ``params``
    - ``query``
    - ``fragment``
    - ``username``
    - ``password``
    - ``port``

    The ``fragment`` is special; it is an array of two elements, the first of which is the ``target_file`` and
    and the second is the ``target_segment``. If there are other parts of the source URL, they must be
    formates as queriy components, and will be parsed into the ``fragment_query``.

    Special application components are:

    - ``proto``. This is set to the ``scheme_extension`` if it exists, the scheme otherwise.
    - ``resource_file``. The filename of the resource to download. It is usually the last part of the URL, but can be overidden in the fragment
    - ``resource_format``. The format name of the resource, normally drawn from the ``resoruce_file`` extension, but can be overidden in the fragment
    - ``target_file``. The filename of the file that will be produced by :py:meth`Url.get_target`, but may be overidden.
    - ``target_format``. The format of the ``target_file``, but may be overidden.
    - ``target_segment``. A sub-component of the ```target_file``, such as the worksheet in a spreadsheet.
    - ``fragment_query``. Holds additional parts of the fragment.

    When the fragment holds extra parts, these can be be formatted as a URL query. Recognized keys are:

    - ``resource_file``
    - ``resource_format``
    - ``target_file``
    - ``target_format``
    - ``encoding``. Text encoding to be used when reading the target.
    - ``headers``. For row-oriented data, the row numbers of the headers, as a comma-seperated list of integers.
    - ``start``. For row-oriented data, the row number of the first row of data ( as opposed to headers. )
    - ``end``. For row-oriented data, the row number of the last row of data.

    """

    match_priority = 100
    match_proto = None
    generator_class = None  # If set, generators match with name = <{generator_class}>

    def __init__(self, url=None, downloader=None, **kwargs):
        """  Initialize a new Application Url
        :param url: URL string
        :param downloader: :py:class:`appurl.web.download.Downloader` object.
        :param kwargs: Additional arguments override URL properties.
        :return: An Application Url object

        Keyword arguments will override properties set by parsing the URL string.

        """

        self._kwargs = kwargs
        self._downloader = downloader

        super().__init__(url, **kwargs)

        assert 'is_archive' not in self._kwargs #?

    def resolve(self):
        """Resolve a URL to another format, such as by looking up a URL that specified a
        search, into another URL. The default implementation returns self. """
        return self

    def get_resource(self):
        """Get the contents of resource and save it to the cache, returning a file-like object"""
        raise NotImplementedError(("get_resource not implemented in {} for '{}'. "
                                   "You may need to install a python mpdule for this type of url")
                                  .format(self.__class__.__name__, str(self)))

    def get_target(self):
        """Get the contents of the target, and save it to the cache, returning a file-like object
        """
        raise NotImplementedError(("get_target not implemented in {} for '{}'"
                                   "You may need to install a python module for this type of url"
                                   )
                                  .format(self.__class__.__name__, str(self)))

    @property
    def downloader(self):
        """Return the Downloader() for this URL"""
        return self._downloader

    def list(self):
        """Return URLS for files contained in an container. This implementation just returns
        ``[self]``, but sub classes may, for instance, list all of the sub-components of a directory,
        or all of the worksheets in an Excel file. """
        return [self]

    @property
    def is_archive(self):
        """Return true if this URL is for an archive. Currently only ZIP is recognized"""
        return self.resource_format in self.archive_formats

    # property
    def archive_file(self):
        """Return the name of the archive file, if there is one."""
        return self.target_file if self.is_archive and self.resource_file != self.target_file else None

    @property
    def fspath(self):
        """The path in a form suitable for use in a filesystem"""
        from pathlib import PurePath
        return PurePath(unquote(self.path))

    @property
    def path_is_absolute(self):
        return self.path.startswith('/')

    def join(self, s):
        """ Join a component to the end of the path, using :func:`os.path.join`. The argument
        ``s`` may be a :class:`appurl.Url` or a string. If ``s`` includes a ``netloc`` property,
        it is assumed to be an absolute url, and it is returned after parsing as a Url. Otherwise,
        the path component of ``s`` is extracted and joined to the path component of this url.

        :param s: A Url object, or a string.
        :return: A copy of this url.
        """

        from copy import copy
        import pathlib

        try:
            path = s.path
            netloc = s.netloc
            u = s
        except AttributeError:
            u = parse_app_url(s, downloader=self.downloader)
            path = u.path
            netloc = u.netloc

        # If there is a netloc, it's an absolute URL
        if netloc:
            return u

        url = copy(self)

        # Using pathlib.PurePosixPath ensures using '/' on windows. os.path.join will use '\'
        url.path = str(pathlib.PurePosixPath(self.path).joinpath(path))

        return url

    def join_dir(self, s):
        """ Join a component to the parent directory of the path, using join(dirname())

        :param s:
        :return: a copy of this url.
        """

        from os.path import dirname
        from copy import copy
        import pathlib

        try:
            path = s.path
            netloc = s.netloc
            u = s
        except AttributeError:
            u = parse_app_url(s, downloader=self.downloader)
            path = u.path
            netloc = u.netloc

        # If there is a netloc, it's an absolute URL
        if netloc:
            return u

        url = copy(self)
        # Using pathlib.PurePosixPath ensures using '/' on windows. os.path.join will use '\'
        url.path = str(pathlib.PurePosixPath(dirname(self.path)).joinpath(path))

        return url

    def join_target(self, tf):
        """Return a new URL, possibly of a new class, with a new target_file"""

        raise NotImplementedError("Not implemented in '{}' ".format(type(self)))

    @property
    def inner(self):
        """Return the URL without the scheme extension and fragment. Re-parses the URL, so it should return
        the correct class for the inner URL. """
        if not self.scheme_extension:
            return self

        c = self.clone(scheme_extension=None, proto=None)

        return parse_app_url(str(c), downloader=self.downloader)


    @property
    def resource_url(self):

        return unparse_url_dict(self.dict,
                                scheme=self.scheme if self.scheme else 'file',
                                scheme_extension=False,
                                fragment_query=False,
                                fragment=False)

    def dirname(self):
        """Return the dirname of the path"""
        from os.path import dirname

        u = self.clone()
        u.path = dirname(self.path)
        return u



    def as_type(self, cls):
        """
        Return the URL transformed to a different class. Copies the downloader and
        build the new url using :py:meth:`Url.dict`

        :param cls: Class of Url to construct
        :return: A new Url object
        """

        return cls(downloader=self.downloader, **self.dict)

    def interpolate(self, context=None):
        """
        Use the Downloader.context to interpolate format strings in the URL. Re-parses the URL,
         returning a new URL

        :param context: Extra context to interpolate with
        :return:
        """

        from copy import copy

        cxt = copy(self.downloader.context)

        cxt.update(context or {})

        from rowgenerators.exceptions import AppUrlError

        try:
            return parse_app_url(str(self).format(**cxt), downloader=self.downloader)
        except KeyError as e:
            raise AppUrlError("Failed to interpolate '{}'; context is {}. Missing key: {} "
                              .format(str(self), self.downloader.context, e))

    def clone(self, **kwargs):
        """
        Return a clone of this Url, possibly with some arguments replaced.

        :param kwargs: Keyword arguments are arguments to set in the copy, using :py:func:`setattr`
        :return: A cloned Url object.
        """
        from copy import deepcopy

        c = deepcopy(self)

        for k, v in kwargs.items():
            try:
                setattr(c, k, v)

            except AttributeError:
                raise AttributeError("Can't set attribute '{}' on '{}' ".format(k, c))

        return c

    @property
    def generator(self):
        """
        Return the generator for this URL, if the rowgenerator package is installed.

        :return: A row generator object.
        """

        from rowgenerators.core import get_generator

        r = self.get_resource()
        t = r.get_target()

        return get_generator(t.get_target(), source_url=self)


    #
    # Matching methods
    #

    def _match_entry_point(self, name):
        """Return true if this URL matches the entrypoint pattern

        Entrypoint patterns:

            'scheme:' Match the URL scheme
            'proto+' Matches the protocol / scheme_extension
            '.ext' Match the resource extension
            '#.ext' Match the target extension
        """

        import re

        if '&' in name:
            return all(self._match_entry_point(n) for n in name.split('&'))

        try:
            name = name.name  # Maybe it's an entrypoint entry, not the name
        except AttributeError:
            pass

        if name == '*':
            return True
        elif name.startswith("/") and name.endswith("/"):
            return re.search(name[1:-1], str(self))
        elif name.endswith(":"):
            return name[:-1] == self.scheme
        elif name.endswith('+'):
            return name[:-1] == self.proto
        elif name.startswith('.'):
            return name[1:] == self.resource_format
        elif name.startswith('#.'):
            return name[2:] == self.target_format
        else:
            return False

    @classmethod
    def _match(cls, url, **kwargs):
        """Return True if this handler can handle the input URL"""
        if cls.match_proto:
            return url.proto == cls.match_proto
        else:
            return True;  # raise NotImplementedError("Match is not implemented for class '{}' ".format(str(cls)))

    #
    # Other support methods
    #


    def __deepcopy__(self, memo):
        return type(self)(None, downloader=self._downloader, **self._parts)

    def __copy__(self):
        return type(self)(None, downloader=self._downloader, **self._parts)

    def _decompose_fragment(self, frag):
        """Parse the fragment component"""
        from urllib.parse import unquote_plus

        if isinstance(frag, (list, tuple)):
            assert frag[0] is None or isinstance(frag[0], str), (frag[0], type(frag[0]))
            return frag

        if not frag:
            return None, None

        frag_parts = unquote_plus(frag).split(';')

        if not frag_parts:
            file, segment = None, None
        elif len(frag_parts) == 1:
            file = frag_parts[0]
            segment = None
        elif len(frag_parts) >= 2:
            file = frag_parts[0]
            segment = frag_parts[1]

        assert file is None or isinstance(file, str), (file, type(file))

        return file, segment

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, str(self))


