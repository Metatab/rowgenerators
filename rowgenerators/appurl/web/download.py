# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

import logging
from functools import lru_cache

_logger = logger = logging.getLogger('rowgenerators.appurl.web.download')


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


class Resource(object):
    cache_path = None
    sys_path = None
    download_time = None

    def __init__(self):
        super().__init__()

    def __str__(self):
        return str(self.__dict__)

def default_downloader_callback(msg_type, message, read_len, total_len):
    pass


class Downloader(object):
    """Downloader objects handle downloading resources from the web, including authorization,
    and storing the downloaded object in a cache. Since they are the primary interface to the file cache,
    all Urls object have a link to a Downloader """

    context = {}  # A variable substitution context, for substituting hostnames, pathnames, etc

    singleton = None

    def __init__(self, cache=None, account_accessor=None, logger=None,
                 working_dir='', callback=None, use_cache=True):
        """
        Download and cache files, via HTTP and FTP, with retry and decompression.

        :param self:
        :param cache: A PyFs filesystem object for caching files
        :param account_accessor: An object for accessing account credentials. Not currently used.
        :param logger: Logging object to write debug logs to
        :param working_dir:
        :param callback: Call back to call with progress reports during downloads.
        :return:
        """

        self._cache = cache

        _logger.debug("__init_ is setting Downloader cache = {}".format(cache))

        self.account_acessor = account_accessor
        self.logger = logger
        self.working_dir = working_dir
        self._callback = callback or default_downloader_callback

        self.use_cache = use_cache # Set to false to ignore cache

        # For debugging singletonness
        #from metapack.util import dump_stack
        #print('======')
        #print(dump_stack(5))

    @staticmethod
    def get_instance(cache=None, account_accessor=None, logger=None,
                     working_dir='', callback=None):
        """Return a memoized singleton"""

        # Wont see this debug statement with the --debug cli command ; it gets called
        # before setting debug log_level
        if not Downloader.singleton:
            _logger.debug("Creating downloader for cache '{}' ".format(cache))
            Downloader.set_singleton(cache, account_accessor, logger, working_dir, callback)

        return Downloader.singleton

    @staticmethod
    def set_singleton(cache=None, account_accessor=None, logger=None,
                     working_dir='', callback=None):
        """Assign the downloader singleton"""

        Downloader.singleton = Downloader(cache, account_accessor, logger, working_dir, callback)
        return Downloader.singleton

    def callback(self, msg_type, message, read_len=-1, total_len=-1 ):
        if True or self._callback:
            self._callback( msg_type, message, read_len, total_len)

    def set_callback(self, cb):
        # The Downloader is supposed to be a singleton, but it gets created all over the place
        self._callback = cb

    def reset_callback(self):
        # The Downloader is supposed to be a singleton, but it gets created all over the place
        self.set_callback(default_downloader_callback)



    @property
    def cache(self):
        if not self._cache:
            from rowgenerators import get_cache
            # qn = self.__module__+'.'+self.__class__.__qualname__
            self._cache = get_cache()

            logger.debug("cache property is setting Downloader cache = {}".format(self._cache))

        return self._cache

    def get_resource(url):
        pass

    def download(self, url):
        from os.path import abspath, join
        from genericpath import exists

        from rowgenerators.appurl.url import Url
        from rowgenerators.exceptions import DownloadError, AccessError

        logger.debug(f"Download {url}, cache is {str(self.cache)}")

        working_dir = self.working_dir if self.working_dir else ''

        r = Resource()

        # For local files, don't download, just reference in place.
        if url.scheme == 'file':
            r.cache_path = Url(url.resource_url).path
            r.download_time = None

            # Many places the file may exist
            locations = {  # What a mess ...
                abspath(r.cache_path),
                abspath(r.cache_path.lstrip('/')),
                abspath(join(working_dir, r.cache_path)),
                abspath(r.cache_path.lstrip('/'))
            }

            for l in locations:
                if exists(l):
                    r.sys_path = l
                    logger.debug("Found '{}'as local file '{}'".format(str(url), l))
                    break
            else:
                raise DownloadError(("File resource does not exist. Found none of:"
                                     "\n{}\n\nWorking dir = {}\ncache_path={}\nspec_path={}")
                                    .format('\n'.join(locations), working_dir, r.cache_path, url.path))

        else:

            # Not a local file, so actually need to download it.
            try:
                r.cache_path, r.download_time = self._download_with_lock(url.resource_url)
            except (AccessError, DownloadError) as e:
                # Try again, using a URL that we may have configured an account for. This is
                # primarily S3 urls, with Boto or AWS credential
                try:
                    r.cache_path, r.download_time = self._download_with_lock(url.auth_resource_url)
                except AttributeError as e:
                    raise e
                except DownloadError as e:
                    raise DownloadError("Access error for url '{}'; also tried accessing as S3 url '{}'".format(url, url.auth_resource_url ))

            r.sys_path = self.cache.getsyspath(r.cache_path)

        logger.debug(f"Downloaded {url} to {r.cache_path}")

        return r

    def cache_path(self, url):
        import hashlib
        from urllib.parse import urlparse
        from os import sep
        from os.path import join, dirname, basename

        url = url.replace('\\', '/')

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
            # We put the hash before the last path element, because that's the target faile, which gets
            # used to figure out what the target format should be.
            cache_path = join(dirname(cache_path), hash, basename(cache_path))

        cache_path  = cache_path.replace(sep,'/')

        return cache_path

    def _download_with_lock(self, url):
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
        from os.path import join
        import time

        from fs.errors import DirectoryExpected, NoSysPath, ResourceInvalid, DirectoryExists
        from requests import HTTPError
        from rowgenerators.exceptions import AccessError, DownloadError

        assert isinstance(url, str)

        cache_path = self.cache_path(url)

        logger.debug(f"_download_with_lock Cache = {self.cache}")
        logger.debug(f"_download_with_lock Path in cache = {cache_path}")


        if not self.cache.exists(cache_path):

            cache_dir = os.path.dirname(cache_path)

            try:
                self.cache.makedirs(cache_dir, recreate=True)
            except DirectoryExpected as e:

                # Probably b/c the dir name is already a file
                dn = os.path.dirname(cache_path)
                bn = os.path.basename(cache_path)
                for i in range(10):
                    try:
                        cache_path = join(dn + str(i), bn)
                        self.cache.makedirs(os.path.dirname(cache_path))
                        break
                    except DirectoryExpected as e2:
                        continue
                    except DirectoryExists:
                        pass  # ? No idea what's supposed to happen here.
                    raise e
                else:
                    # Exhausted all of the trial values
                    raise e

        try:
            from filelock import FileLock
            lock = FileLock(self.cache.getsyspath(cache_path + '.lock'))

        except NoSysPath:
            # mem: caches, and others, don't have sys paths.
            # FIXME should check for MP operation and raise if there would be
            # contention. Mem  caches are only for testing with single processes
            lock = _NoOpFileLock()

        with lock:
            if self.cache.exists(cache_path):
                # Rather than ignoring the cache when use_cache is False, we
                # delete the file and re-download it, because if you ignore the cached file,
                # you still have to download the resource to a file somewhere.
                if self.use_cache:
                    logger.debug(f"Found {cache_path} in cache, and cache is active")
                    return cache_path, None
                else:
                    logger.debug(f"File {cache_path} is not in cache")
                    try:
                        logger.debug(f"Found {cache_path} in cache, but cache not active; deleting")
                        self.cache.remove(cache_path)
                    except ResourceInvalid:
                        pass  # Well, we tried.

            try:
                self._download(url, cache_path)

                return cache_path, time.time()

            except HTTPError as e:
                if e.response.status_code == 403:
                    raise AccessError("Access error on download: {}".format(e))
                else:
                    raise DownloadError("Failed to download: {}".format(e))

            except (KeyboardInterrupt, Exception):
                # This is really important -- its really bad to have partly downloaded
                # files being confused with fully downloaded ones.
                # FIXME. Should also handle signals. deleting partly downloaded files is important.
                # Maybe should have a sentinel file, or download to another name and move the
                # file after done.
                if self.cache.exists(cache_path):
                    self.cache.remove(cache_path)

                raise

        assert False, 'Should never get here'

    def _download(self, url, cache_path):
        import requests
        import functools
        from urllib.request import urlopen

        from requests.exceptions import SSLError

        from rowgenerators.appurl.util import parse_url_to_dict, copy_file_or_flo
        from rowgenerators.exceptions import DownloadError
        from ftplib import FTP

        self.callback('download', url)

        if url.startswith('s3:'):

            from rowgenerators import parse_app_url

            s3url = parse_app_url(url)

            try:
                with self.cache.open(cache_path, 'wb') as f:
                    s3url.object.download_fileobj(f)
            except Exception as e:
                raise DownloadError("Failed to fetch S3 url '{}': {}".format(url, e))

        elif url.startswith('ftp:'):

            logger.debug("Fetch " + str(url))

            u = parse_url_to_dict(url)

            try:
                with FTP(u['netloc']) as ftp, self.cache.open(cache_path, 'wb') as fout:

                    total_len = [0]

                    def _read(d):
                        fout.write(d)

                        total_len[0] = total_len[0] + len(d)

                        self.callback('ftp read', url, len(d), total_len[0])

                    ftp.login()
                    ftp.retrbinary('RETR ' + u['path'], _read)
                    ftp.quit()
            except ConnectionError as e:
                raise DownloadError("Failed to get FTP url '{}': {} ".format(url, e))

        else:

            logger.debug("Request " + str(url))

            headers = {}

            try:
                r = requests.get(url, headers=headers, stream=True)
                r.raise_for_status()
            except SSLError as e:
                raise DownloadError("Failed to GET {}: {} ".format(url, e))

            if r.status_code>=300:
                if r.status_code>=304:
                    raise DownloadError(f"Server Returned 304: Not Modified. for {str(url)}");
                else:
                    raise DownloadError(f"Can't handle server response, {r.status_code}");

            # Requests will auto decode gzip responses, but not when streaming. This following
            # monkey patch is recommended by a core developer at
            # https://github.com/kennethreitz/requests/issues/2155
            if r.headers.get('content-encoding') in ('gzip', 'br'):
                r.raw.read = functools.partial(r.raw.read, decode_content=True)

            def copy_cb(message, read_len, total_len):
                # Message is just the read len
                self.callback('copy', url, read_len, total_len)

            logger.debug(f"Response code={r.status_code} {r.headers}" )

            with self.cache.open(cache_path, 'wb') as f:
                copy_file_or_flo(r.raw, f, cb=copy_cb)

            assert self.cache.exists(cache_path)

