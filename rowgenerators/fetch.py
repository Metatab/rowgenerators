# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import functools
import hashlib
import ssl
from os.path import abspath, exists

from requests import HTTPError
from six import string_types
from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen

from rowgenerators.exceptions import MissingCredentials, SourceError
from rowgenerators.generators import *
from rowgenerators.util import fs_join as join
from .s3 import AltValidationS3FS
from .util import real_files_in_zf, copy_file_or_flo, parse_url_to_dict, get_cache


def download_and_cache(spec, cache_fs, account_accessor=None, clean=False, logger=None, working_dir='', callback=None):

    parts = {}

    working_dir = working_dir if working_dir else ''

    if spec.scheme == 'file':
        parts['cache_path'] = parse_url_to_dict(spec.resource_url)['path']
        parts['download_time'] = None

        locations = [ # What a mess ...
            abspath(parts['cache_path']),
            abspath(parts['cache_path'].lstrip('/')),
            abspath(join(working_dir, parts['cache_path'])),
            abspath(parts['cache_path'].lstrip('/'))
        ]

        for l in locations:
            if exists(l):
                parts['sys_path'] = l
                break
        else:
            raise IOError("File resource does not exist. Found none of:\n{}\n\nWorking dir = {}\ncache_path={}\nspec_path={}"
                          .format('\n'.join(locations), working_dir, parts['cache_path'], spec.path))

    else:
        cache_fs = cache_fs or get_cache()

        parts['cache_path'], parts['download_time'] = download(spec.resource_url, cache_fs, account_accessor,
                                                               clean=clean, logger=logger, callback=callback)

        parts['sys_path'] = cache_fs.getsyspath(parts['cache_path'])

    return parts


def get_file_from_zip(d, spec):
    from zipfile import ZipFile
    import re

    zf = ZipFile(d['sys_path'])

    nl = list(zf.namelist())

    if spec.target_file:
        names = list([e for e in nl if re.search(spec.target_file, e)
                      and not (e.startswith('__') or e.startswith('.'))
                      ])
        if len(names) > 0:
            return names[0]

    if spec.target_segment:
        try:
            return nl[int(spec.target_segment)]

        except (IndexError, ValueError):
            pass

    return nl[0]


def _download(url, cache_fs, cache_path, account_accessor, logger, callback=None):
    import urllib
    import requests
    from fs.errors import ResourceNotFound

    def copy_callback(read, total):
        # if callback:
        #    callback('copy_file',read, total)
        pass

    if callback:
        callback('download', url, 0)

    if url.startswith('s3:'):
        s3 = get_s3(url, account_accessor)
        pd = parse_url_to_dict(url)

        try:
            with cache_fs.open(cache_path, 'wb') as fout:
                with s3.open(urllib.unquote_plus(pd['path']), 'rb') as fin:
                    copy_file_or_flo(fin, fout, cb=copy_callback)
        except ResourceNotFound:
            raise ResourceNotFound("Failed to find path '{}' in S3 FS '{}' ".format(pd['path'], s3))

    elif url.startswith('ftp:'):
        from contextlib import closing

        with closing(urlopen(url)) as fin:

            with cache_fs.open(cache_path, 'wb') as fout:

                read_len = 16 * 1024
                total_len = 0
                while 1:
                    buf = fin.read(read_len)
                    if not buf:
                        break
                    fout.write(buf)
                    total_len += len(buf)

                    if callback:
                        copy_callback(len(buf), total_len)

    else:

        r = requests.get(url, stream=True)
        r.raise_for_status()

        # Requests will auto decode gzip responses, but not when streaming. This following
        # monkey patch is recommended by a core developer at
        # https://github.com/kennethreitz/requests/issues/2155
        if r.headers.get('content-encoding') == 'gzip':
            r.raw.read = functools.partial(r.raw.read, decode_content=True)

        with cache_fs.open(cache_path, 'wb') as f:
            copy_file_or_flo(r.raw, f, cb=copy_callback)

        assert cache_fs.exists(cache_path)


def download(url, cache_fs, account_accessor=None, clean=False, logger=None, callback=None):
    """
    Download a URL and store it in the cache.

    :param url:
    :param cache_fs:
    :param account_accessor: callable of one argument (url) returning dict with credentials.
    :param clean: Remove files from cache and re-download
    :param logger:
    :param callback:
    :return:
    """
    import os.path
    import time
    from fs.errors import DirectoryExpected, NoSysPath, ResourceInvalid, DirectoryExists

    assert isinstance(url, string_types)

    url = url.replace('\\','/')

    # .decode('utf8'). The fs modulegets upset when given strings, so 
    # we need to decode to unicode. UTF8 is a WAG.
    try:
        parsed = urlparse(url.decode('utf8'))
    except AttributeError:
        parsed = urlparse(url)

    # Create a name for the file in the cache, based on the URL
    # the '\' replacement is because pyfs only wants to use UNIX path seperators, but
    # python os.path.join will use the one specified for the operating system.
    cache_path = join(parsed.netloc, parsed.path.strip('/'))

    # If there is a query, hash it and add it to the path
    if parsed.query:
        hash = hashlib.sha224(parsed.query.encode('utf8')).hexdigest()
        cache_path = join(cache_path, hash)

    if not cache_fs.exists(cache_path):

        cache_dir = os.path.dirname(cache_path)

        try:
            cache_fs.makedirs(cache_dir, recreate=True)
        except DirectoryExpected as e:

            # Probably b/c the dir name is already a file
            dn = os.path.dirname(cache_path)
            bn = os.path.basename(cache_path)
            for i in range(10):
                try:
                    cache_path = join(dn + str(i), bn)
                    cache_fs.makedirs(os.path.dirname(cache_path))
                    break
                except DirectoryExpected:
                    continue
                except DirectoryExists:
                    pass # ? No idea what's supposed to happen here.
                raise e

    try:
        from filelock import FileLock
        lock = FileLock(cache_fs.getsyspath(cache_path + '.lock'))

    except NoSysPath:
        # mem: caches, and others, don't have sys paths.
        # FIXME should check for MP operation and raise if there would be
        # contention. Mem  caches are only for testing with single processes
        lock = _NoOpFileLock()

    with lock:
        if cache_fs.exists(cache_path):
            if clean:
                try:
                    cache_fs.remove(cache_path)
                except ResourceInvalid:
                    pass  # Well, we tried.
            else:
                return cache_path, None

        try:
            _download(url, cache_fs, cache_path, account_accessor, logger, callback)

            return cache_path, time.time()

        except HTTPError as e:
            raise SourceError("Failed to download: {}".format(e))

        except (KeyboardInterrupt, Exception):
            # This is really important -- its really bad to have partly downloaded
            # files being confused with fully downloaded ones.
            # FIXME. Should also handle signals. deleteing partly downloaded files is important.
            # Maybe should have a sentinel file, or download to another name and move the
            # file after done.
            if cache_fs.exists(cache_path):
                cache_fs.remove(cache_path)

            raise

    assert False, 'Should never get here'


def get_s3(url, account_accessor):
    """ Gets file from s3 storage.

    Args:
        url (str): url of the file
        account_accessor (callable): callable returning dictionary with s3 credentials (access and secret
            at least)

    Example:
        get_s3('s3://example.com/file1.csv', lambda url: {'access': '<access>': 'secret': '<secret>'})

    Returns:
        S3FS instance (file-like):
    """

    # The monkey patch fixes a bug: https://github.com/boto/boto/issues/2836

    _old_match_hostname = ssl.match_hostname

    # FIXME. This issue is possibly better handled with
    # https://pypi.python.org/pypi/backports.ssl_match_hostname
    def _new_match_hostname(cert, hostname):
        if hostname.endswith('.s3.amazonaws.com'):
            pos = hostname.find_first('.s3.amazonaws.com')
            hostname = hostname[:pos].replace('.', '') + hostname[pos:]
        return _old_match_hostname(cert, hostname)

    ssl.match_hostname = _new_match_hostname

    pd = parse_url_to_dict(url)

    if account_accessor is None or not six.callable(account_accessor):
        raise TypeError('account_accessor argument must be callable of one argument returning dict.')

    account = account_accessor(pd['netloc'])
    # Direct access to the accounts file yeilds 'access', but in the Accounts ORM object, its 'access_key'
    aws_access_key = account.get('access', account.get('access_key'))
    aws_secret_key = account.get('secret')

    missing_credentials = []
    if not aws_access_key:
        missing_credentials.append('access')
    if not aws_secret_key:
        missing_credentials.append('secret')

    if missing_credentials:
        raise MissingCredentials(
            'dict returned by account_accessor callable for {} must contain not empty {} key(s)'
                .format(pd['netloc'], ', '.join(missing_credentials)),
            location=pd['netloc'], required_credentials=['access', 'secret'], )

    s3 = AltValidationS3FS(
        bucket=pd['netloc'],
        # prefix=pd['path'],
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    )

    # ssl.match_hostname = _old_match_hostname

    return s3


def get_gs(url, segment, account_acessor):
    """
    Old code for accessing google spreadsheets, with authentication
    :param url:
    :param segment:
    :param account_acessor:
    :return:
    """
    import gspread
    from oauth2client.client import SignedJwtAssertionCredentials
    from gspread.exceptions import WorksheetNotFound

    json_key = account_acessor('google_spreadsheets')

    scope = ['https://spreadsheets.google.com/feeds']

    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

    spreadsheet_key = url.replace('gs://', '')

    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(spreadsheet_key)

    try:
        return sh.worksheet(segment)
    except WorksheetNotFound:
        raise SourceError("Failed to find worksheet specified by segment='{}' Spreadsheet has: {} ".format(
            segment, [e.title for e in sh.worksheets()]))


class _NoOpFileLock(object):
    """No Op for pyfilesystem caches where locking wont work"""

    def __init__(self, lf):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            raise exc_val

    def acquire(self):
        pass

    def release(self):
        pass


def enumerate_contents(base_spec, cache_fs, callback=None):
    """Inspect the URL, and if it is a container ( ZIP Or Excel ) inspect each of the contained
    files. Yields all of the lefel-level URLs"""
    from rowgenerators import SourceSpec

    if not isinstance(base_spec, SourceSpec):
        base_spec = SourceSpec(url=base_spec)

    for s in inspect(base_spec, cache_fs, callback=callback):
        for s2 in inspect(s, cache_fs, callback=callback):
            yield s2


def inspect(ss, cache_fs, callback=None):
    """Return a list of possible extensions to the url, such as files within a ZIP archive, or
    worksheets in a spreadsheet"""
    from zipfile import ZipFile
    from copy import deepcopy
    from rowgenerators.generators import  get_generator

    d = download_and_cache(ss, cache_fs, callback=callback)

    if callback:
        callback("Inspecting: format={} file={} segment={} url={}".format(
            ss.target_format, ss.target_file, ss.target_segment, ss.rebuild_url()))

    if ss.is_archive and ss.target_file is None:

        zf = ZipFile(cache_fs.getsyspath(d['cache_path']))

        if ss.target_file is None:
            l = []

            for file_name in real_files_in_zf(zf):
                l.append(ss.update(target_file=file_name))

            return l

        elif ss.target_format in ('xls', 'xlsx') and ss.target_segment is None:
            src = get_generator(ss, cache_fs)
            l = []
            for seg in src.children:
                ss2 = ss.update(target_segment=seg)
                l.append(ss2)

            return l

    if ss.target_format in ('xls', 'xlsx') and ss.target_segment is None:

        src = get_generator(ss, cache_fs)

        l = []
        for seg in src.children:
            ss2 = ss.update(target_segment=seg)
            l.append(ss2)

        return l

    return [deepcopy(ss)]
