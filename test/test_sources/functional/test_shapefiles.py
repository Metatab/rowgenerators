# -*- coding: utf-8 -*-

from ambry_sources import get_source
from ambry_sources.mpf import MPRowsFile

from fs.opener import fsopendir
import pytest

from tests import TestBase


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    @pytest.mark.slow
    def test_highways(self):
        # FIXME: Optimize to use local file instead of downloading it all the time.
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        spec = sources['highways']
        source = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

        # first check is it converted properly.
        row_gen = source._get_row_gen()
        first_row = next(row_gen)

        # generates valid first row
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row[0], 0)
        # last element is wkt.
        self.assertIn('LINESTRING', first_row[-1])

        # header is valid
        self.assertEqual(len(source._headers), 68)
        self.assertEqual(source._headers[0], 'id')
        self.assertEqual(source._headers[-1], 'geometry')

        # now check its load to MPRows
        mpr = MPRowsFile(cache_fs, spec.name).load_rows(source)

        # Are columns recognized properly?
        NAME_INDEX = 1  # which element of the column description contains name.
        # Collect all names from column descriptors. Skip first elem of the schema because
        # it's descriptor of column descriptor elements.
        columns = [x[NAME_INDEX] for x in mpr.meta['schema'][1:]]
        self.assertIn('id', columns)
        self.assertIn('geometry', columns)
        self.assertIn('length', columns)  # column from shape file.

        # Is first row valid?
        first_row = next(iter(mpr.reader))
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row['id'], 0)
        self.assertIn('LINESTRING', first_row['geometry'])

        return

        # spec columns are properly populated
        self.assertEqual(len(spec.columns), 68)
        self.assertEqual(spec.columns[0]['name'], 'id')
        self.assertEqual(spec.columns[-1]['name'], 'geometry')

    @pytest.mark.slow
    def test_all(self):
        """ Test all sources from geo_sources.csv """
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        for name, spec in sources.items():
            if name == 'highways':
                # it is already tested. Skip.
                continue

            source = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

            # now check its load to MPRows
            mpr = MPRowsFile(cache_fs, spec.name).load_rows(source)
            first_row = next(iter(mpr.reader))

            # Are columns recognized properly?

            NAME_INDEX = 1  # which element of the column description contains name.
            # Collect all names from column descriptors. Skip first elem of the schema because
            # it's descriptor of column descriptor elements.
            columns = [x[NAME_INDEX] for x in mpr.meta['schema'][1:]]
            self.assertIn('id', columns)
            self.assertIn('geometry', columns)

            # Is first row valid?
            self.assertEqual(len(columns), len(first_row))
