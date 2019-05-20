from __future__ import print_function

import unittest

from rowgenerators.appurl.url import parse_app_url
from rowgenerators.appurl.util import parse_file_to_uri, parse_url_to_dict

from rowgenerators.appurl.archive.zip import ZipUrl
from rowgenerators.appurl.file.csv import CsvFileUrl
from rowgenerators.appurl.web.web import WebUrl

class TestIssues(unittest.TestCase):

    def test_spec_resource_format(self):

        us='http://public.source.civicknowledge.com/example.com/sources/test_data.foo#simple-example.csv&resource_format=zip'

        u = parse_app_url(us)

        self.assertEqual('zip', u.resource_format)
        self.assertEqual('csv',u.target_format)

        r = u.get_resource()
        self.assertIsInstance(r, ZipUrl)

        t = r.get_target()
        self.assertIsInstance(t, CsvFileUrl)


    def test_csv_no_csv(self):
        u = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/simple-example.foo#&target_format=csv')

        self.assertIsInstance(u, WebUrl)
        self.assertEqual('foo', u.resource_format)
        self.assertEqual('csv', u.target_format)

        r = u.get_resource()
        self.assertEqual('foo', r.resource_format)
        self.assertEqual('csv', r.target_format)

        t = r.get_resource()
        self.assertEqual('csv', t.target_format)


    def test_excel_renter07(self):

        u = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/renter_cost_excel07.zip#target_format=xlsx')

        r = u.get_resource()
        self.assertEqual('file', r.proto)
        self.assertTrue(r.exists(), r)

        self.assertEqual('renter_cost_excel07.zip', u.target_file)

        t = r.get_target()
        self.assertEqual('file', t.proto)
        self.assertTrue(t.exists())


    def test_mz_with_zip_xl(self):
        u = parse_app_url(
            'http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx')

        self.assertIsInstance(u, WebUrl)
        self.assertEqual('zip', u.resource_format)
        self.assertEqual('xlsx', u.target_format)

        r = u.get_resource()
        self.assertIsInstance(r, ZipUrl)
        self.assertEqual('zip', r.resource_format)
        self.assertEqual('file', r.proto)
        self.assertTrue(r.exists(), r.fspath)


        t = r.get_target()

        self.assertEqual('xlsx', t.target_format)
        self.assertEqual('file', t.proto)
        self.assertTrue(t.exists())


    def test_xlsx_fragment(self):

        url = parse_app_url('http://example.com/renter_cost_excel07.xlsx#2')

        self.assertEqual('2', url.target_file)

    def test_xsx_zip_fragment(self):

        url = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx;Sheet1')

        self.assertEqual('renter_cost_excel07.xlsx', url.target_file)
        self.assertEqual('Sheet1', url.target_segment)

    def test_join_target_xls(self):
        u = parse_app_url('file:/a/file.xlsx')

        jt = u.join_target('target')

        self.assertEquals('file:/a/file.xlsx#target', str(jt))

    def test_join_target_xls(self):
        from rowgenerators.appurl.file.excel import ExcelFileUrl

        u = parse_app_url('file:/a/file.xlsx#foobnar')

        self.assertIsInstance(u, ExcelFileUrl)

    def test_parse_s3(self):
        from rowgenerators.appurl.web.s3 import S3Url

        u = parse_app_url(('s3://library.metatab.org'))

        self.assertIsInstance(u, S3Url)


    def test_downloaded_resource_type(self):

        u = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/test_data.zip')

        ru = u.get_resource()

        self.assertIsInstance(ru, ZipUrl)

    def x_test_Url_parsing(self):
        from rowgenerators import Url
        from rowgenerators.appurl.util import parse_url_to_dict, unparse_url_dict

        us = '/a/b/c'

        d = parse_url_to_dict(us)
        print(d)

        print(unparse_url_dict(d))

        u = Url(us)
        print(u.dict)
        print(unparse_url_dict(u.dict))

    def test_second_frag_args(self):

        url = parse_app_url('http://example.com/file.txt#encoding=latin1&target_format=tsv&start=10')

        self.assertEqual('latin1',url.encoding)
        self.assertEqual('tsv',url.target_format)
        self.assertEqual('10', url.start)

    def test_not_passing_target_format(self):

        url = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/simple-example.foo')
        url.target_format = 'csv'

        ru = url.get_resource()
        tu = ru.get_target()

        print(tu)
        print(tu.target_format)

        return

        print(url.get_resource().get_target().target_format)

        self.assertEqual(url.target_format,'csv')
        self.assertEqual(url.get_resource().target_format, 'csv')
        self.assertEqual(url.get_resource().get_target().target_format, 'csv')

        url = parse_app_url('http://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2017s.txt#target_format=tsv')

        self.assertEqual(url.target_format,'tsv')
        self.assertEqual(url.get_resource().target_format, 'tsv')
        self.assertEqual(url.get_resource().get_target().target_format, 'tsv')

        url = parse_app_url('http://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2017s.txt#encoding=foo&target_format=tsv')

        self.assertEqual(url.target_format,'tsv')
        self.assertEqual(url.get_resource().target_format, 'tsv')
        self.assertEqual(url.get_resource().get_target().target_format, 'tsv')



    def test_set_query_args(self):
        from rowgenerators.appurl.url import Url, UrlParts


        url = UrlParts('/example.com/sources/unicode-utf8.csv')
        print(type(url))
        url.encoding = 'bar'
        url.target_format = 'bar'

        url = parse_app_url('/example.com/sources/unicode-utf8.csv'
                            '#encoding=foo&target_format=foo&start=10')

        print(type(url))
        url.encoding = 'bar'
        url.target_format = 'bar'

        url = parse_app_url('http://public.source.civicknowledge.com/example.com/sources/unicode-utf8.csv'
                            '#encoding=foo&target_format=foo&start=10')

        print(type(url))
        url.encoding='bar'
        url.target_format = 'bar'

        print(url)

        ru = url.get_resource()
        tu = ru.get_target()

        import pprint
        pprint.pprint(ru.__dict__)

        return

        self.assertEqual('bar', url._encoding)
        self.assertEqual('bar', url.encoding)
        self.assertEqual('bar', ru.encoding)
        self.assertEqual('bar', tu.encoding)

        self.assertEqual('bar', url._target_format)
        self.assertEqual('bar', url.target_format)
        self.assertEqual('bar', ru.target_format)
        self.assertEqual('bar', tu.target_format)

        from rowgenerators.appurl.web import WebUrl

        url = WebUrl('http://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2017s.txt')
        print(url.target_format)
        url.target_format = 'foo'
        print(url._target_format)
        print(url.target_format)

    def x_test_broken_shape(self):

        from itertools import islice

        url = parse_app_url('shape+file:///Users/eric/proj/virt-proj/data-project/gis-projects/sandiegodata.org'
                            '-stormdrains/data/Drain_Structure.zip')

        print(type(url), url)

        t = url.get_resource().get_target()

        print(type(t), t)

        for row in t.generator:
            print(row[-1])

    def x_test_zip_fragment_query(self):

        fn = '../data/adult_2017_stata.zip#ADULT.dta&values=codes'

        fn='/Users/eric/proj/virt-proj/data-project/chis/data/adult_2017_stata.zip#ADULT.dta&values=codes'

        u = parse_app_url(fn)

        self.assertTrue('ADULT.dta',u.target_file)

        t = u.get_target()

        self.assertTrue('codes', t.fragment_query['values'])

    def test_zip_fragment(self):


        self.assertEqual('file:///foo/bar/archive.zip#excel.xls',
                           parse_file_to_uri('/foo/bar/archive.zip#excel.xls'))

        self.assertEqual('file:///foo/bar/archive.zip#excel.xls',
                          parse_file_to_uri('file:///foo/bar/archive.zip#excel.xls'))


    def test_excel_fragment(self):

        us = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx'
        u = parse_app_url(us)
        r = u.get_resource()
        t = r.get_target()

        print(u)
        print(r)
        print(t)

        self.assertEqual('renter_cost_excel07.xlsx',t.target_file)
        self.assertIsNone(t.target_segment)


        us = 'https://www.cde.ca.gov/ds/sd/sd/documents/cupc1314.xls#1'
        u = parse_app_url(us)
        r = u.get_resource()
        t = r.get_target()

        print(u)
        print(r)
        print(t)



    def test_join_target(self):
        from pprint import pprint
        from rowgenerators.appurl.url import UrlParts, Url

        us = 'http://public.source.civicknowledge.com/example.com/sources/test_data.foo#simple-example.csv&resource_format=zip'

        u = parse_app_url(us)

        ru = u.get_resource()
        print(ru)

        pprint(ru.resource_format)
        pprint(ru._parts)



    def test_urlparts_descriptors(self):
        from rowgenerators.appurl.url import UrlParts, Url

        up = UrlParts('http://example.com/foobar')

        print(up.path)
        up.path = 'path'
        print(up.path)

        up.scheme = 1
        up.username = 1

if __name__ == '__main__':
    unittest.main()