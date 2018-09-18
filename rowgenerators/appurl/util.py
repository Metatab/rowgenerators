
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """




def path2url(path):
    "Convert a pathname to a file URL"

    from urllib.parse import urljoin
    from urllib.request import pathname2url


    return urljoin('file:', pathname2url(path))

def parse_file_to_uri(url):
    """If this is a file path, return a Path object, otherwise, return None"""
    import pathlib
    from urllib.parse import urlparse
    import re

    url = str(url)

    p = urlparse(url)

    if p.scheme == 'file':
        return url

    elif re.match(r'^[a-zA-Z]:', url): # Has a drive letter
        return pathlib.PureWindowsPath(url.replace('\\','/')).as_uri()

    elif url.startswith('//') or url.startswith('\\\\'): # Windows share
        return pathlib.PureWindowsPath(url.replace('\\', '/')).as_uri()

    elif not p.scheme: # No scheme, but can't use .as_uri if relative
        try:
            return pathlib.PurePath(url.replace('\\', '/')).as_uri()
        except ValueError:
            return 'file:'+url

    else:
        return None

def parse_url_to_dict(url, assume_localhost=False):
    """Parse a url and return a dict with keys for all of the parts.

    The urlparse function() returns a wacky combination of a namedtuple
    with properties.

    """

    import re
    from urllib.parse import unquote_plus, ParseResult, urlparse, parse_qs

    assert url is not None

    url = str(parse_file_to_uri(url) or url)

    p = urlparse(url)

    #  '+' indicates that the scheme has a scheme extension
    if '+' in p.scheme:
        scheme_extension, scheme = p.scheme.split('+')
    else:
        scheme = p.scheme
        scheme_extension = None

    if scheme is '':
        scheme = 'file'

    frag_whole = unquote_plus(p.fragment) if p.fragment else ''

    frag_parts = frag_whole.split('&', 1)

    if frag_parts and '=' not in frag_parts[0]:
        frag = frag_parts.pop(0)
    else:
        frag = None

    frag_rem = frag_parts.pop(0) if frag_parts else None

    # parse_qs returns lists for values, since queries can have multiple keys with different values,
    # but we expect unique values
    frag_query = { k:v[0] for k, v in  (parse_qs(frag_rem) if p.fragment else {}).items()  }

    if frag:
        frag_sub_parts = frag.split(';')

        if len(frag_sub_parts) < 2:
            frag_sub_parts = [frag_sub_parts[0], None]

    else:
        frag_sub_parts = [None, None]

    # Urlparse converts the hostname to lowercase, which we'd prefer it not do when the
    # hostname is specified as an interpoation string.
    # Note that this will only work if the hostname is entirely uppercase, and will fail if
    # it is mixed case.

    if p.hostname and (p.hostname[0] == '{' and p.hostname[-1] == '}' and p.hostname.upper() in url):
        p_hostname = p.hostname.upper()
    else:
        p_hostname = p.hostname

    def unmangle_windows_path(scheme, path):
        import re
        if scheme == 'file' and re.match("/[a-zA-Z]:", path):
            return path.lstrip('/')
        else:
            return path

    return {
        'scheme': scheme,
        'scheme_extension': scheme_extension,
        'netloc': p.netloc,
        'hostname': p_hostname,
        'path': unmangle_windows_path(scheme,p.path),
        'params': p.params,
        'query': p.query,
        'fragment':  frag_sub_parts,
        'fragment_query': frag_query,
        'username': p.username,
        'password': p.password,
        'port': p.port
    }

def unparse_fragment(d, **kwargs):

    from urllib.parse import quote_plus, urlencode, unquote

    if d.get('fragment') or d.get('fragment_query'):

        if isinstance(d.get('fragment'),(list, tuple)):

            seg = ';'.join(quote_plus(str(e)) for e in [ e for e in d.get('fragment') if e])
        else:
            seg = quote_plus(d.get('fragment'))

        if d.get('fragment_query'):
            fqt = sorted(d.get('fragment_query').items())
            query = '&' + urlencode(fqt,doseq=True)
        else:
            query = ''


        if seg or query:
            return "#"+seg+unquote(query)

    return ''

def unparse_url_dict(d, **kwargs):
    from urllib.parse import quote_plus, urlencode, unquote
    import re

    d = dict(d.items())

    d.update(kwargs)

    if '_fragment' in d and 'fragment' not in d:
        d['fragment'] = d['_fragment']


    # Using netloc preserves case for the host, which the host value does not do,
    # but netloc also will have the username, password and port in it
    if d.get('netloc') and ('@' not in d['netloc'] and ':' not in d['netloc']):
        if 'netloc' in d and d['netloc']:
            host_port = d['netloc']
        else:
            host_port = ''
    else:
        host_port = d['hostname']

    if 'port' in d and d['port']:
        host_port += ':' + str(d['port'])

    user_pass = ''
    if 'username' in d and d['username']:
        user_pass += d['username']

    if 'password' in d and d['password']:
        user_pass += ':' + d['password']

    if user_pass:
        host_port = '{}@{}'.format(user_pass, host_port)

    if d.get('scheme') == 'file':
        if d['netloc']: # Windows UNC path
            url = 'file://{}/{}'.format(d['netloc'], d.get('path', '').lstrip('/'))

        elif d.get('path','').startswith('/'): # absolute path
            url = 'file://{}'.format( d.get('path', ''))

        elif re.match('[a-zA-Z]:/', d.get('path','')): # windows path
            url = 'file:///{}'.format(d.get('path', ''))

        else: # relative path
            url = 'file:{}'.format( d.get('path', ''))

    elif d.get('scheme') == 'mailto':
        url = '{}:{}'.format(d['scheme'], d.get('path', ''))

    elif d.get('scheme') and host_port:
        url = '{}://{}/{}'.format(d['scheme'], host_port, d.get('path', '').lstrip('/'))

    elif d.get('scheme'):
        url = '{}://{}'.format(d['scheme'], d.get('path', '').lstrip('/'))

    elif d.get('path'):
        raise AppUrlError("Can only unparse file urls that have a file: scheme")
        # It's possibly just a local file url.
        # This isn't the standard file: url form, which is specified to have a :// and a host part,
        # like 'file://localhost/etc/config', but that form can't handle relative URLs ( which don't start with '/')
        # url = 'file:'+d['path']
    else:
        url = ''

    if d.get('scheme_extension'):
        url = d['scheme_extension']+'+'+url

    if 'query' in d and d['query']:
        url += '?' + d['query']

    url = url + unparse_fragment(d,**kwargs)

    return url

def reparse_url(url, **kwargs):

    assume_localhost = kwargs.get('assume_localhost', False)

    return unparse_url_dict(parse_url_to_dict(url,assume_localhost),**kwargs)

def join_url_path(url, *paths):
    """Like path.os.join, but operates on the url path, ignoring the query and fragments."""

    import os

    parts = parse_url_to_dict(url)

    return reparse_url(url, path=os.path.join(parts['path']))

def file_ext(v):
    """Split of the extension of a filename, without throwing an exception of there is no extension. Does not
    return the leading '.'
    :param v: """

    from os.path import splitext

    try:
        try:
            v = splitext(v)[1][1:]
        except TypeError:
            v = splitext(v.fspath)[1][1:]

        if v == '*':  # Not a file name, probably a fragment regex
            return None

        return v.lower() if v else None
    except IndexError:
        return None



def copy_file_or_flo(input_, output, buffer_size=64 * 1024, cb=None):
    """ Copy a file name or file-like-object to another file name or file-like object"""

    from os import makedirs
    from os.path import isdir, dirname

    assert bool(input_)
    assert bool(output)

    input_opened = False
    output_opened = False

    try:
        if isinstance(input_, str):

            if not isdir(dirname(input_)):
                makedirs(dirname(input_))

            input_ = open(input_, 'r')
            input_opened = True

        if isinstance(output, str):

            if not isdir(dirname(output)):
                makedirs(dirname(output))

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
                    cb(len(buf), len(buf), cumulative)

        copyfileobj(input_, output)

    finally:
        if input_opened:
            input_.close()

        if output_opened:
            output.close()


DEFAULT_CACHE_NAME = 'rowgen-cache'

def get_cache_name(cache_name=None):
    cn = cache_name or DEFAULT_CACHE_NAME

    if not cn:
        from rowgenerators.exceptions import ConfigurationError

        raise ConfigurationError("Must either set the default cache name or call cache functions with a name")

    return cn

def set_default_cache_name(cache_name):
    global DEFAULT_CACHE_NAME
    DEFAULT_CACHE_NAME = cache_name

def get_cache(cache_name=None, clean=False):
    """Return the path to a file cache"""

    from fs.osfs import OSFS
    from fs.appfs import UserDataFS
    from fs.errors import CreateFailed
    import os

    cache_name = get_cache_name(cache_name)

    # If the environmental variable for the cache is set, change the cache directory.
    env_var = (cache_name+'_cache').upper()

    cache_dir = os.getenv(env_var, None)

    if cache_dir:
        try:
            return OSFS(cache_dir)
        except CreateFailed as e:
            raise CreateFailed("Failed to create '{}': {} ".format(cache_dir, e))
    else:

        try:
            return UserDataFS(cache_name.lower())
        except CreateFailed as e:
            raise CreateFailed("Failed to create '{}': {} ".format(cache_name.lower(), e))



def clean_cache(cache = None, cache_name=None):
    """Delete items in the cache older than 24 hours"""
    import datetime

    cache = cache if cache else get_cache( get_cache_name(cache_name))

    ignores = ['index.json', 'index.json.bak']

    for step in cache.walk.info():
        details = cache.getdetails(step[0])
        mod = details.modified
        now = datetime.datetime.now(tz=mod.tzinfo)
        age = (now - mod).total_seconds()
        if age > (60 * 60 * 24) and details.is_file:
            if step[0] not in ignores:
                cache.remove(step[0])

def nuke_cache(cache = None, cache_name=None):
    """Delete Everythong in the cache"""

    cache = cache if cache else get_cache(get_cache_name(cache_name))

    for step in cache.walk.info():
        if not step[1].is_dir:
            cache.remove(step[0])

def ensure_dir(path):
    from os import makedirs
    from os.path import exists

    if path and not exists(path):
            makedirs(path)


def import_name_or_class(name):
    " Import an obect as either a fully qualified, dotted name, "

    if isinstance(name, str):

        # for "a.b.c.d" -> [ 'a.b.c', 'd' ]
        module_name, object_name = name.rsplit('.',1)
        # __import__ loads the multi-level of module, but returns
        # the top level, which we have to descend into
        mod = __import__(module_name)


        components = name.split('.')

        for comp in components[1:]: # Already got the top level, so start at 1
            mod = getattr(mod, comp)


        return mod
    else:
        return name # Assume it is already the thing we want to import
