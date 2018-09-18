# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """


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
        classes = sorted([ep.load() for ep in iter_entry_points(group='appurl.urls') if u._match_entry_point(ep.name)],
                         key=lambda cls: cls.match_priority)
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


class Url(object):
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

    # Basic URL components
    scheme = None
    scheme_extension = None
    netloc = None
    hostname = None
    _path = None
    params = None
    query = None
    fragment = [None, None]
    fragment_query = {}
    username = None
    password = None
    port = None

    # Application components
    _proto = None
    _resource_file = None
    _resource_format = None
    _target_file = None
    _target_format = None
    _target_segment = None

    encoding = None  # target encoding
    headers = None  # line number of headers
    start = None  # start line for data
    end = None  # end line for data

    match_priority = 100
    match_proto = None
    generator_class = None  # If set, generators match with name = <{generator_class}>

    def __init__(self, url=None, downloader=None, **kwargs):
        """  Initialize a new Application Url
        :param url: URL string
        :param downloader: :py:class:`appurl.web.download.Downloader` object.
        :param kwargs: Additional arguments override URL properties.
        :return: An Application Url object


        Keyword arguments will override properties set by parsing the URL string. Valid keywords
        that will set object properties are listed below. Other keyswords are accepted and ignored

        - scheme
        - scheme_extension
        - netloc
        - hostname
        - path
        - params
        - fragment
        - fragment_query
        - username
        - password
        - port

        """

        from .util import parse_url_to_dict

        assert 'is_archive' not in kwargs

        self._kwargs = kwargs

        if url is not None:

            parts = parse_url_to_dict(url)

            for k, v in parts.items():
                try:
                    # print(" {}: '{}' ".format(k,v))
                    setattr(self, k, v)
                except AttributeError:
                    print("Can't Set: ", k, v)

        else:
            for k in "scheme scheme_extension netloc hostname path params query fragment fragment_query username " \
                     "password port".split():

                if k == 'fragment_query' and kwargs.get(k) is None:  # Probably trying to set it to Null
                    setattr(self, k, {})
                else:
                    v =  kwargs.get(k)
                    if isinstance(v, str):
                        v = v.strip()

                    setattr(self, k, v)


        self.fragment_query = kwargs.get('fragment_query', self.fragment_query or {})

        self._fragment = self._decompose_fragment(kwargs.get('fragment', self.fragment))

        assert self._fragment[0] is None or isinstance(self._fragment[0], str), type(self._fragment[0])

        if not self._fragment:
            self._fragment = [None, None]


        self.scheme_extension = kwargs.get('scheme_extension', self.scheme_extension)

        self.scheme = kwargs.get('scheme', self.scheme)

        self._proto = kwargs.get('proto', self.proto)
        self._resource_file = kwargs.get('resource_file')
        self._resource_format = kwargs.get('resource_format', self.fragment_query.get('resource_format'))
        self._target_format = kwargs.get('target_format', self.fragment_query.get('target_format'))
        self._target_segment = kwargs.get('target_segment')

        self.encoding = kwargs.get('encoding', self.fragment_query.get('encoding', self.encoding))
        self.headers = kwargs.get('headers', self.fragment_query.get('headers', self.headers))
        self.start = kwargs.get('start', self.fragment_query.get('start', self.start))
        self.end = kwargs.get('end', self.fragment_query.get('end', self.end))

        try:
            self._target_format = self._target_format.lower()
        except AttributeError:
            pass

        self._downloader = downloader

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
                                   "You may need to install a python mpdule for this type of url"
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
    def path(self):
        return self._path


    @path.setter
    def path(self,v):
        self._path = v

    @property
    def fspath(self):
        """The path in a form suitable for use in a filesystem"""
        from pathlib import PurePath
        return PurePath(self.path)

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

        from os.path import join, dirname
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

        return parse_app_url(str(self.clone(scheme_extension=None)), downloader=self.downloader)

    def dirname(self):
        """Return the dirname of the path"""
        from os.path import dirname

        u = self.clone()
        u.path = dirname(self.path)
        return u

    def clear_fragment(self):
        """
        Return a copy of the URL with no fragment components

        :return: A cloned URl object, with the fragment and fragment queries cleared.
        """

        c = self.clone()
        c.fragment = [None, None]
        c.fragment_query = {}
        c.encoding = None
        c.start = None
        c.end = None
        c.headers = None
        return c

    def as_type(self, cls):
        """
        Return the URL transformed to a different class. Copies the downloader and
        build the new url using :py:meth:`Url.dict`

        :param cls: Class of Url to construct
        :return: A new Url object
        """

        return cls(downloader=self.downloader, **self.dict)



    @property
    def dict(self):
        """
        Returns a dictionary of the object components.

        :return: a dict.
        """
        self._update_parts()
        keys = "scheme scheme_extension netloc hostname path params query _fragment fragment_query username password " \
               "port proto  resource_format  target_format " \
               "encoding target_segment".split()

        d = dict((k, getattr(self,k)) for k in keys)


        return d

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
        Return a clone of this Url, popssibly with some arguments replaced.

        :param kwargs: Keyword arguments are arguments to set in the copy, using :py:func:`setattr`
        :return: A cloned Url object.
        """

        d = self.dict.copy()
        c = type(self)(None, downloader=self._downloader, **d)
        c._kwargs = self._kwargs
        c.fragment = self.fragment

        c._update_parts()

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
    # Property accessors
    #

    @property
    def fragment(self):
        return self._fragment

    @fragment.setter
    def fragment(self, v):
        """Set the fragment in place"""
        assert isinstance(v, (list, tuple, type(None), str)), v

        if isinstance(v, str):
            # One string is the target_file
            self._fragment = [v, None]
        elif isinstance(v, (list, tuple)):
            self._fragment = list(v)
        else:
            self._fragment = [None, None]

    def set_fragment(self, f):
        """Return a clone with the fragment set"""
        u = self.clone()
        u.fragment = f
        return u

    @property
    def proto(self):
        return self._proto or \
               self.scheme_extension or \
               {'https': 'http', '': 'file'}.get(self.scheme) or \
               self.scheme

    @property
    def resource_url(self):
        from .util import unparse_url_dict

        return unparse_url_dict(self.dict,
                                scheme=self.scheme if self.scheme else 'file',
                                scheme_extension=False,
                                fragment_query=False,
                                fragment=False)

    @property
    def resource_file(self):

        from os.path import basename

        if self.path:
            return basename(self.path)
        else:
            return None

    @property
    def resource_format(self):

        from .util import file_ext

        if self._resource_format:
            return self._resource_format
        elif not self.resource_file:
            return None
        else:
            return file_ext(self.resource_file)

    @property
    def target_file(self):

        if self._target_file:
            return self._target_file

        try:
            if self.fragment[0]:
                return self.fragment[0]
        except IndexError:
            pass



        return self.resource_file

    @target_file.setter
    def target_file(self, v):
        self.fragment[0] = v

    def set_target_file(self, v):
        """Return a clone with a target_file set"""
        u = self.clone()
        u.fragment[0] = v
        return u

    @property
    def target_segment(self):
        if self.fragment:
            return self.fragment[1]
        else:
            return None

    @target_segment.setter
    def target_segment(self, v):
        self.fragment[1] = v

    def set_target_segment(self, v):
        """Return a clone with a target_file set"""
        u = self.clone()
        u.fragment[1] = v
        return u

    @property
    def target_format(self):
        from .util import file_ext

        target_format = None

        if self._target_format:
            target_format = self._target_format

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
    def target_format(self, target_format):
        self._target_format = target_format

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

        if '&' in name:
            return all(self._match_entry_point(n) for n in name.split('&'))

        try:
            name = name.name  # Maybe it's an entrypoint entry, not the name
        except AttributeError:
            pass

        if name == '*':
            return True
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

    def _update_parts(self):
        """Update the fragement_query. Set the attribute for the query value to False to delete it from
        the fragment query"""

        for k in "encoding headers start end".split():
            if getattr(self, k):
                self.fragment_query[k] = getattr(self, k)
            elif getattr(self, k) == False and k in self.fragment_query:
                del self.fragment_query[k]

                # if self.fragment:
                #    self.rebuild_fragment()

    def __deepcopy__(self, memo):
        d = self.dict.copy()
        d.update(self._kwargs)
        return type(self)(None, downloader=self._downloader, **d)

    def __copy__(self):
        d = self.dict.copy()
        d.update(self._kwargs)
        return type(self)(None, downloader=self._downloader, **d)

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

    def __str__(self):

        from .util import unparse_url_dict

        self._update_parts()
        return unparse_url_dict(self.dict)
