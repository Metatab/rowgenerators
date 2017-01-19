# -*- coding: utf-8 -*-
"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


from six import string_types, text_type
import os


class DelayedFlo(object):
    """Holds functions to open and close a flike-like object"""

    def __init__(self, path, open_f, flo_f, close_f):
        self.path = path
        self.open_f = open_f
        self.flo_f = flo_f
        self.close_f = close_f
        self.memo = None
        self.message = None # Set externally for debugging

    def open(self, mode):
        self.memo = self.open_f(mode)
        return self.flo_f(self.memo)

    def close(self):
        if self.memo:
            self.close_f(self.memo)



def copy_file_or_flo(input_, output, buffer_size=64 * 1024, cb=None):
    """ Copy a file name or file-like-object to another file name or file-like object"""

    assert bool(input_)
    assert bool(output)

    input_opened = False
    output_opened = False

    try:
        if isinstance(input_, string_types):

            if not os.path.isdir(os.path.dirname(input_)):
                os.makedirs(os.path.dirname(input_))

            input_ = open(input_, 'r')
            input_opened = True

        if isinstance(output, string_types):

            if not os.path.isdir(os.path.dirname(output)):
                os.makedirs(os.path.dirname(output))

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
                    cb(len(buf), cumulative)

        copyfileobj(input_, output)

    finally:
        if input_opened:
            input_.close()

        if output_opened:
            output.close()

def parse_url_to_dict(url):
    """Parse a url and return a dict with keys for all of the parts.

    The urlparse function() returns a wacky combination of a namedtuple
    with properties.

    """
    from six.moves.urllib.parse import urlparse, urlsplit, urlunsplit

    p = urlparse(url)

    #  '+' indicates that the scheme has a scheme extension
    if '+' in p.scheme:

        scheme_extension, scheme = p.scheme.split('+')

    else:
        scheme = p.scheme
        scheme_extension = None

    return {
        'scheme': scheme,
        'scheme_extension': scheme_extension,
        'netloc': p.netloc,
        'path': p.path,
        'params': p.params,
        'query': p.query,
        'fragment': p.fragment,
        'username': p.username,
        'password': p.password,
        'hostname': p.hostname,
        'port': p.port
    }

def unparse_url_dict(d):

    if 'netloc' in d and d['netloc']:
        host_port = d['netloc']
    else:
        host_port = ''

    if 'port' in d and d['port']:
        host_port += ':' + text_type(d['port'])

    user_pass = ''
    if 'username' in d and d['username']:
        user_pass += d['username']

    if 'password' in d and d['password']:
        user_pass += ':' + d['password']

    if user_pass:
        host_port = '{}@{}'.format(user_pass, host_port)

    if d.get('scheme') and host_port:
        url = '{}://{}/{}'.format(d['scheme'],host_port, d.get('path', '').lstrip('/'))
    elif d.get('path'):
        # It's possible just a local file url.
        url = d['path']
    else:
        url = ''

    if d.get('scheme_extension'):
        url = d['scheme_extension']+'+'+url

    if 'query' in d and d['query']:
        url += '?' + d['query']

    return url