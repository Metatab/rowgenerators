# -*- coding: utf-8 -*-

from ambry_sources import get_source
from ambry_sources.mpf import MPRowsFile

from fs.opener import fsopendir
import pytest

from tests import TestBase


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_ctor(self):

        d = '/tmp/socrata'

        from os import makedirs
        from os.path import exists
        from shutil import rmtree

        if exists(d):
            print "Make", d
            rmtree(d)

        makedirs(d)

        cache_fs = fsopendir(d)  # fsopendir(self.setup_temp_dir())


        sources = self.load_sources(file_name='sources.csv')
        spec = sources['facilities']
        source = get_source(spec, cache_fs)

        def cb(*args):
            print args

        mpr = MPRowsFile(cache_fs, spec.name).load_rows(source, callback = cb, limit = 10)




