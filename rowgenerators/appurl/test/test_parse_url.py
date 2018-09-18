from __future__ import print_function

import unittest



from rowgenerators.appurl.url import parse_app_url

from rowgenerators.appurl.web.download import Downloader



class TestUrlParse(unittest.TestCase):
    def test_fragment(self):
        def u(frag):
            return parse_app_url("http://example.com/foo.csv#" + frag)

        self.assertEqual({}, u('a').fragment_query)
        self.assertEqual({}, u('a;b').fragment_query)
        self.assertEqual({'foo': 'bar'}, u('a;b&foo=bar').fragment_query)
        self.assertEqual({'foo': 'bar'}, u('a;&foo=bar').fragment_query)
        self.assertEqual({'foo': 'bar'}, u('a&foo=bar').fragment_query)
        self.assertEqual({'foo': 'bar'}, u('&foo=bar').fragment_query)
        self.assertEqual({'foo': 'bar'}, u(';&foo=bar').fragment_query)

        url = u('a;b&encoding=utf8')

        self.assertEqual('utf8', url.encoding)

        url.encoding = 'ascii'
        url.start = 5

        self.assertEqual('http://example.com/foo.csv#a;b&encoding=ascii&start=5', str(url))

        url = u('a;b&target_format=foo&resource_format=bar')

        self.assertEqual('foo', url.target_format)
        self.assertEqual('bar', url.resource_format)

        us = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip#unicode-latin1.csv&encoding=latin1'

        url = parse_app_url(us)

        self.assertEqual('latin1', url.encoding)
        self.assertEqual('latin1', url.get_resource().encoding)
        self.assertEqual('latin1', url.get_resource().get_target().encoding)

    def test_fragment_2(self):
        url = parse_app_url(
            'http://public.source.civicknowledge.com/example.com/sources/renter_cost_excel07.zip#target_format=xlsx')

        self.assertEqual('zip', url.resource_format)
        self.assertEqual('renter_cost_excel07.zip', url.target_file)
        self.assertEqual('xlsx', url.target_format)

        r = url.get_resource()

        self.assertEqual('xlsx', r.target_format)
        self.assertEqual('zip', r.resource_format)

    def test_two_extensions(self):
        u_s = 'http://public.source.civicknowledge.com/example.com/sources/simple-example.csv.zip'

        u = parse_app_url(u_s, Downloader.get_instance())

        self.assertEqual('simple-example.csv.zip', u.resource_file)
        self.assertEqual('simple-example.csv.zip', u.target_file)
        self.assertEqual('zip', u.target_format)

        r = u.get_resource()

        self.assertEqual('simple-example.csv.zip', r.resource_file)
        self.assertEqual('simple-example.csv', r.target_file)


        self.assertEqual('csv', r.target_format)

    def test_shapefile_url(self):
        from rowgenerators.appurl.web.web import WebUrl

        u_s = 'shapefile+http://public.source.civicknowledge.com.s3.amazonaws.com/example.com/geo/Parks_SD.zip'

        u = parse_app_url(u_s)

        self.assertIsInstance(u, WebUrl, type(u))

        r = u.get_resource()

        self.assertTrue(str(r).startswith('shapefile+'))

    def test_s3_url(self):
        from rowgenerators.appurl.web.s3 import S3Url

        url_str = 's3://bucket/a/b/c/file.csv'

        u = parse_app_url(url_str)

        self.assertIsInstance(u, S3Url, type(u))

    def test_python_url(self):
        from rowgenerators.appurl.file import python
        from rowgenerators import get_generator
        from types import ModuleType

        import sys

        foo = ModuleType('foo')
        sys.modules['foo'] = foo
        foo.bar = ModuleType('bar')
        sys.modules['foo.bar'] = foo.bar
        foo.bar.baz = ModuleType('baz')
        sys.modules['foo.bar.baz'] = foo.bar.baz

        def foobar(*args, **kwargs):
            for i in range(10):
                yield i

        foo.bar.baz.foobar = foobar

        u = parse_app_url("python:foo.bar.baz#foobar")

        g = get_generator(u)

        self.assertEqual(45, sum(list(g)))

    def test_sql_url(self):

        url = """oracle://coredev:{CREATIONS_PW}@creations.bc.edu:1521/SISCNV#"FINAID"."COMMENTS_TEXT_LINE_RECORD" """

        u = parse_app_url(url)

        g = u.generator

        print(type(g), g)

        dsn_url = "oracle://coredev:{CREATIONS_PW}@creations.bc.edu:1521/SISCNV"

        url = 'sql://creations#sql://creations#SELECT * FROM  "FINAID"."COMMENTS_TEXT_LINE_RECORD"'

        dsn_dict = { 'creations' : dsn_url}

        u = parse_app_url(url, dsns=dsn_dict).get_resource().get_target()

        print(type(u), u)


    def test_interpolate(self):

        from rowgenerators.appurl.web.download import Downloader

        Downloader.context = {
            'PASSWORD' : 'correcthorse',
            'HOST': 'sajak'
        }

        u = parse_app_url('http://user:{PASSWORD}@{HOST}/foo/bar')

        print(u.interpolate())

    def test_maintain_target(self):

        b = 'https://www2.census.gov/programs-surveys/acs/summary_file/2016/data/5_year_seq_by_state/RhodeIsland/Tracts_Block_Groups_Only'

        us = b+'/20165ri0001000.zip#m20165ri0001000.txt&target_format=csv'


        u = parse_app_url(us)

        print (u)



if __name__ == '__main__':
    unittest.main()
