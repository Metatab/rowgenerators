
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """



import unittest
try:
    import fiona, pyproj, shapely
    test_geo = True
except:
    test_geo = False

from rowgenerators import get_generator, parse_app_url



class TestGeo(unittest.TestCase):

    def setUp(self):
        import warnings
        warnings.simplefilter('ignore')

    @unittest.skipIf(not test_geo,"These tests require modules: fiona, pyproj, shapely")
    def test_geo(self):
        from rowgenerators.generator.shapefile import ShapefileSource
        from rowgenerators.appurl.file.shapefile import ShapefileUrl

        us = 'shape+http://s3.amazonaws.com/public.source.civicknowledge.com/sangis.org/Subregional_Areas_2010.zip'
        u = parse_app_url(us)

        r = u.get_resource()

        self.assertIsInstance(r, ShapefileUrl)

        t = r.get_target()

        self.assertIsInstance(t, ShapefileUrl)

        self.assertTrue(
            str(t).endswith('public.source.civicknowledge.com/sangis.org/Subregional_Areas_2010.zip#SRA2010tiger.shp'))

        g = get_generator(t)

        self.assertIsInstance(g, ShapefileSource)

        self.assertEqual([{'name': 'id', 'type': 'int'}, {'name': 'SRA', 'type': 'int'},
                          {'name': 'NAME', 'type': 'str'}, {
                              'name': 'geometry', 'type': 'geometry_type'}], g.columns)
        self.assertEqual(['id', 'SRA', 'NAME', 'geometry'], g.headers)

        self.assertEqual(42, len(list(g)))

    def test_geoframe(self):

        us = 'shape+http://s3.amazonaws.com/public.source.civicknowledge.com/sangis.org/Subregional_Areas_2010.zip'
        u = parse_app_url(us)

        #df = u.generator.dataframe()
        #print(df.head())
         
        print(u.generator.geoframe().geometry.total_bounds)