# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT, included in this distribution as LICENSE

""" """

from rowgenerators.appurl.file.file import FileUrl
from rowgenerators.exceptions import AppUrlError


class ZipUrlError(AppUrlError):
    pass


class ZipUrl(FileUrl):
    """Zip URLS represent a zip file, as a local resource. """

    match_priority = FileUrl.match_priority - 10

    def __init__(self, url=None, downloader=None, **kwargs):

        super().__init__(url, downloader=downloader, **kwargs)

        if self.resource_format != 'zip':
            self.resource_format = 'zip'


    @property
    def target_file(self):
        """
        Returns the target file, which is usually stored in the first slot in the ``fragment``,
        but may have been overridden with a ``fragment_query``.

        :return:
        """

        if self._parts['target_file']:
            return self._parts['target_file']

        for ext in ('csv', 'xls', 'xlsx'):
            if self.resource_file.endswith('.' + ext + '.zip'):
                return self.resource_file.replace('.zip', '')

        # Want to return none, so get_files_from-zip can assume to use the first file in the archive.
        return None

    # Just a copy of the one from Url; looks like it must be reset because
    # the target_file prop was replaced
    @target_file.setter
    def target_file(self, v):
        self._parts['target_file'] = v

    def join_target(self, tf):
        """
        Joins the target ``tf`` by setting the value of the first slot of the fragment.

        :param tf:
        :return: a clone of this url with a new fragment.
        """
        u = self.clone()

        try:
            u.target_file = str(tf.path)
        except AttributeError:
            u.target_file = tf

        return u

    def get_resource(self):
        return self

    @property
    def zip_dir(self):
        """Directory that files will be extracted to"""

        from os.path import abspath

        cache_dir = self.downloader.cache.getsyspath('/')
        target_path = abspath(self.fspath)

        if target_path.startswith(cache_dir):  # Case when file is already in cache
            return str(self.fspath) + '_d'
        else:  # file is not in cache; it may exist elsewhere.
            return self.downloader.cache.getsyspath(target_path.lstrip('/'))+'_d'

    def get_target(self):
        """
        Extract the target file from the archive, store it in the cache, and return a file Url to the
        cached file.

        """

        from rowgenerators.appurl.url import parse_app_url
        from zipfile import ZipFile, BadZipFile
        from rowgenerators.appurl.util import copy_file_or_flo, ensure_dir
        from pathlib import Path
        from time import time


        assert self.zip_dir

        try:
            zf = ZipFile(str(self.fspath))
        except BadZipFile:
            raise ZipUrlError(f"Not a zip file: {str(self.fspath)} for url {str(self)}")

        self.target_file = ZipUrl.get_file_from_zip(self)

        target_path = Path(self.zip_dir).joinpath(self.target_file)
        ensure_dir(target_path.parent)

        def age(p):
            """File age in minutes"""
            try:
                return (time() - p.stat().st_mtime) / 60
            except:
                print("XXXX", p, p.exists())
                raise

        # Unpack the file if it dos not exist, or if the zile file is newer
        if not target_path.exists() or age(target_path) > age(Path(self.fspath)):
            with target_path.open('wb') as f, zf.open(self.target_file) as flo:
                copy_file_or_flo(flo, f)
            disp = 'copied'
        else:
            disp = 'extant'

        fq = self.frag_dict

        if 'resource_format' in fq:
            del fq['resource_format']

        if 'resource_file' in fq:
            del fq['resource_file']

        if 'target_file' in fq:
            del fq['target_file']

        tu =  parse_app_url(str(target_path),
                             scheme_extension=self.scheme_extension,
                             downloader=self.downloader,
                             **fq
                             )

        if self.target_format != tu.target_format:

            try:
                tu.target_format = self.target_format
            except AttributeError:
                pass # Some URLS don't allow resetting target type.

        tu._disp = disp

        return tu

    def list(self):
        """List the files in the referenced Zip file"""

        from zipfile import ZipFile


        if self.target_file:
            return list(self.set_target_segment(tl.target_segment) for tl in self.get_target().list())
        else:
            real_files = ZipUrl.real_files_in_zf(ZipFile(str(self.fspath)))
            return list(self.set_target_file(rf) for rf in real_files)

    @staticmethod
    def get_file_from_zip(url):
        """Given a file name that may be a regular expression, return the full name for the file
        from a zip archive"""

        from rowgenerators.exceptions import AppUrlError
        from zipfile import ZipFile, BadZipFile
        import re

        names = []
        try:
            zf = ZipFile(str(url.fspath))
        except BadZipFile:
            raise AppUrlError(f"Bad zip file: '{str(url.fspath)}' ")

        nl = list(ZipUrl.real_files_in_zf(zf))  # Old way, but maybe gets links? : list(zf.namelist())

        tf = url.target_file
        ts = url.target_segment

        if not nl:
            # sometimes real_files_in_zf doesn't work at all. I don't know why it does work,
            # so I certainly don't know why it does not.
            nl = list(zf.namelist())

        # the target_file may be a string, or a regular expression

        if tf:

            if tf.startswith('*'):
                # Common user error using a glob instead of a regex
                tf = tf.replace('*', '.*')

            try:
                names = list([e for e in nl if re.search(tf, e)
                          and not (e.startswith('__') or e.startswith('.'))
                          ])
            except Exception as e:
                raise

            if len(names) > 0:
                return names[0]


        # The segment, if it exists, can only be an integer, and should probably be
        # '0' to indicate the first file. This clause is probably a bad idea, since
        # andy other integer is probably meaningless.
        if ts:
            try:
                return nl[int(ts)]

            except (IndexError, ValueError):
                pass

        # Just return the first file in the archive.
        if not tf and not ts:
            return nl[0]
        else:

            raise ZipUrlError("Could not find file in Zip {} for target='{}' nor segment='{}'"
                              .format(url.fspath, url.target_file, url.target_segment))

    @staticmethod
    def real_files_in_zf(zf):
        """Return a list of internal paths of real files in a zip file, based on the 'external_attr' values"""
        from os.path import basename

        for e in zf.infolist():

            # Get rid of __MACOS and .DS_whatever
            if basename(e.filename).startswith('__') or basename(e.filename).startswith('.'):
                continue

            # I really don't understand external_attr, but no one else seems to either,
            # so we're just hacking here.
            # e.external_attr>>31&1 works when the archive has external attrs set, and a dir heirarchy
            # e.external_attr==0 works in cases where there are no external attrs set
            # e.external_attr==32 is true for some single-file archives.
            # e.external_attr==128 has been true at least once ...
            if bool(e.external_attr >> 31 & 1 or e.external_attr == 0 or e.external_attr == 32 or e.external_attr == 128):
                yield e.filename


    @classmethod
    def _match(cls, url, **kwargs):

        return url.resource_format == 'zip' or kwargs.get('force_archive')
