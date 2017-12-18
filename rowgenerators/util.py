# -*- coding: utf-8 -*-
"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import collections
import datetime
import os
import re
import unicodedata
from functools import wraps
from os.path import basename
from os.path import join

from six import string_types
from six import text_type
from urllib.parse import urljoin, urlparse, quote_plus, unquote_plus, ParseResult
from urllib.request import pathname2url

from rowgenerators import get_cache, clean_cache, nuke_cache

# Keep code cleanups from deleteing these they are imported her
# because they got moved to appurl
assert clean_cache
assert get_cache
assert nuke_cache



def real_files_in_zf(zf):
    """Return a list of internal paths of real files in a zip file, based on the 'external_attr' values"""


    for e in zf.infolist():

        if basename(e.filename).startswith('__') or basename(e.filename).startswith('.'):
            continue

        # I really don't understand external_attr, but no one else seems to either,
        # so we're just hacking here.
        # e.external_attr>>31&1 works when the archive has external attrs set, and a dir heirarchy
        # e.external_attr==0 works in cases where there are no external attrs set
        # e.external_attr==32 is true for some single-file archives.
        if bool(e.external_attr >> 31 & 1 or e.external_attr == 0 or e.external_attr == 32):
            yield e.filename


def fs_join(*args):
    """Like os.path.join, but never returns '\' chars"""

    return join(*args).replace('\\','/')



def qualified_class_name(o):
    """Full name of an object, including the module"""
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + '.' + o.__class__.__name__

# From http://stackoverflow.com/a/295466
def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.type(
    """
    import re
    import unicodedata
    from six import text_type
    value = text_type(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf8').strip().lower()
    value = re.sub(r'[^\w\s\-\.]', '', value)
    value = re.sub(r'[-\s]+', '-', value)
    return value

def memoize(obj):
    cache = obj.cache = {}

    @wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]

    return memoizer


class Constant:
    """Organizes constants in a class."""

    class ConstError(TypeError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const(%s)" % name)
        self.__dict__[name] = value