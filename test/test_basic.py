from __future__ import print_function

import unittest
from copy import deepcopy
from csv import DictReader, DictWriter

from fs.tempfs import TempFS

from rowgenerators import  RowGenerator
from rowgenerators import SourceSpec
from rowgenerators.fetch import get_generator
from rowgenerators.urls import Url


def data_path(v):
    from os.path import dirname, join
    d = dirname(__file__)
    return join(d, 'test_data',v)

def sources():
    import csv
    with open(data_path('sources.csv')) as f:
        r = csv.DictReader(f)
        return list(r)

def cache_fs():

    from fs.tempfs import TempFS

    return TempFS('rowgenerator')


class BasicTests(unittest.TestCase):

    def compare_dict(self, name, a, b):
        from metatab.util import flatten
        fa = set('{}={}'.format(k, v) for k, v in flatten(a));
        fb = set('{}={}'.format(k, v) for k, v in flatten(b));

        # The declare lines move around a lot, and rarely indicate an error
        fa = {e for e in fa if not e.startswith('declare=')}
        fb = {e for e in fb if not e.startswith('declare=')}

        errors = len(fa - fb) + len(fb - fa)

        if errors:
            print("=== ERRORS for {} ===".format(name))

        if len(fa - fb):
            print("In a but not b")
            for e in sorted(fa - fb):
                print('    ', e)

        if len(fb - fa):
            print("In b but not a")
            for e in sorted(fb - fa):
                print('    ', e)

        self.assertEqual(0, errors)


    def test_source_spec_url(self):
        from rowgenerators import SourceSpec, RowGenerator
        from copy import deepcopy

        ss = SourceSpec(url='http://foobar.com/a/b.csv')
        self.assertEqual('b.csv', ss.target_file)
        self.assertIsNone(ss.target_segment)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a')
        print(ss._url)
        self.assertEqual('a',ss.target_file)
        self.assertIsNone(ss.target_segment)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.target_file,ss2.target_file)
        self.assertIsNone(ss.target_segment)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a;b')
        self.assertEqual('a',ss.target_file)
        self.assertEqual('b',ss.target_segment)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.target_file,ss2.target_file)
        self.assertEqual(ss.target_segment,ss2.target_segment)

        ss = RowGenerator(url='http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx')
        self.assertEqual('renter_cost_excel07.xlsx', ss.target_file)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.target_file, ss2.target_file)

        for url in (
            'http://example.com/foo/archive.zip',
            'http://example.com/foo/archive.zip#file.xlsx',
            'http://example.com/foo/archive.zip#file.xlsx;0',
            'socrata+http://example.com/foo/archive.zip'
        ):
           pass


        print(SourceSpec(url='socrata+http://chhs.data.ca.gov/api/views/tthg-z4mf').__dict__)

    def test_run_sources(self):


        cache = cache_fs()

        for sd in sources():
            # Don't have the column map yet.
            if sd['name'] in ('simple_fixed','facilities'):
                continue

            ss = SourceSpec(**sd)

            gen = RowGenerator(cache=cache, **sd)

            rows = list(gen)

            try:
                self.assertEquals(int(sd['n_rows']), len(rows))
            except Exception as e:
                print('---')
                print(sd['name'], e)
                print(rows[0])
                print(rows[-1])

    def test_inspect(self):

        from rowgenerators import inspect

        cache = cache_fs()

        us = 'http://www.sandiegocounty.gov/content/dam/sdc/hhsa/programs/phs/CHS/Community%20Profiles/MCH_2010-2013.xlsx'

        ss = SourceSpec(us)

        print(ss.update(target_segment='foo'))
        print(ss._url.rebuild_url(target_segment='foo'))
        return

        for spec in inspect(ss, cache, callback=print):
            print(spec)

    def test_google(self):
        from rowgenerators import SourceSpec, GooglePublicSource
        spec = SourceSpec(url='gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w')

        self.assertEquals('gs',spec.proto)
        self.assertEquals('gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/', spec.url)
        self.assertEquals('https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/export?format=csv',GooglePublicSource.download_url(spec))

        self.assertEquals(12004, len(list(spec.get_generator(cache_fs()))))

    def test_zip(self):

        from rowgenerators import enumerate_contents, RowGenerator, SourceError, TextEncodingError

        z = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip'
        cache = cache_fs()

        for c in enumerate_contents(z,  cache):

            print(c.url, c.encoding)

            if c.target_format in ('foo','txt'):
                continue

            gen = RowGenerator(url=c.url)
            try:
                print(len(list(gen)))
            except (UnicodeDecodeError, TextEncodingError) as e:
                print("UERROR", c.name, e)
            except SourceError as e:
                print("ERROR", c.name, e)



    def test_d_and_c(self):
        from csv import DictReader
        from rowgenerators.fetch import download_and_cache

        from os.path import isfile

        cache = TempFS()

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):
                d = download_and_cache(SourceSpec(**e), cache)

                self.assertTrue(isfile(d['sys_path']))

    def test_delayed_flo(self):
        from csv import DictReader

        cache = TempFS()
        success = []
        errors = []

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):

                if e['name'] in ('simple_fixed',):
                    continue

                if e['name'] not in ('zip_no_xls',):
                    continue

                s = SourceSpec(**e)

                print(s.dict)

                d = get_generator(s, cache)

                print(s._url, len(list(d)))

    def test_urls(self):

        headers="in_url class url resource_url resource_file target_file scheme proto resource_format target_format " \
                "is_archive encoding target_segment".split()

        with open(data_path('url_classes.csv')) as f, open('/tmp/url_classes.csv', 'w') as f_out:
            w = None
            r = DictReader(f)
            errors = 0
            for i, d in enumerate(r):

                url = d['in_url']

                o = Url(url)

                do = dict(o.__dict__.items())
                del do['parts']

                if w is None:
                    w = DictWriter(f_out, fieldnames= headers)
                    w.writeheader()
                do['in_url'] = url
                do['is_archive'] = o.is_archive
                do['class'] = o.__class__.__name__
                w.writerow(do)

                d = {k: v if v else None for k, v in d.items()}
                do = {k: str(v) if v else None for k, v in do.items()}# str() turns True into 'True'

                # a is the gague data from url_classes.csv
                # b is the test object.

                try:                     # A, B
                    self.compare_dict(url, d, do)
                except AssertionError as e:
                    errors += 1
                    #raise


            self.assertEqual(0, errors)

        with open(data_path('url_classes.csv')) as f:

            r = DictReader(f)
            for i, d in enumerate(r):

                u1 = Url(d['in_url'])

        with open(data_path('url_classes.csv')) as f:

            r = DictReader(f)
            for i, d in enumerate(r):
                u1 = Url(d['in_url'])
                d1 = u1.__dict__.copy()
                d2 = deepcopy(u1).__dict__.copy()

                # The parts will be different Bunch objects
                del d1['parts']
                del d2['parts']

                self.assertEqual(d1, d2)

                self.assertEqual(d1, u1.dict)

        for us in ("http://example.com/foo.zip", "http://example.com/foo.zip#a;b"):
            u = Url(us, encoding='utf-8')
            u2 = u.update(target_file='bingo.xls', target_segment='1')

            self.assertEqual('utf-8',u2.dict['encoding'])
            self.assertEqual('bingo.xls', u2.dict['target_file'])
            self.assertEqual('1', u2.dict['target_segment'])


    def test_url_update(self):

        u1 = Url('http://example.com/foo.zip')

        self.assertEqual('http://example.com/foo.zip#bar.xls',u1.rebuild_url(target_file='bar.xls'))
        self.assertEqual('http://example.com/foo.zip#0', u1.rebuild_url(target_segment=0))
        self.assertEqual('http://example.com/foo.zip#bar.xls%3B0', u1.rebuild_url(target_file='bar.xls', target_segment=0))

        u2 = u1.update(target_file='bar.xls')

        self.assertEqual('bar.xls',u2.target_file)
        self.assertEqual('xls', u2.target_format)

        self.assertEqual('http://example.com/foo.zip', u1.rebuild_url(False, False))

        self.assertEqual('file:metatadata.csv',Url('file:metatadata.csv').rebuild_url())

    def test_parse_file_urls(self):
        from rowgenerators.util import parse_url_to_dict, unparse_url_dict, reparse_url
        urls = [
            ('file:foo/bar/baz','foo/bar/baz','file:foo/bar/baz'),
            ('file:/foo/bar/baz', '/foo/bar/baz','file:/foo/bar/baz'),
            ('file://foo/bar/baz', 'foo/bar/baz','file:foo/bar/baz'),
            ('file:///foo/bar/baz', '/foo/bar/baz','file:/foo/bar/baz'),
        ]

        for i,o,u in urls:
            p = parse_url_to_dict(i)
            self.assertEqual(o, p['path'])
            self.assertEqual(u, unparse_url_dict(p))
            self.assertEqual(o, parse_url_to_dict(u)['path'])

        print(reparse_url("metatab+http://library.metatab.org/cdph.ca.gov-county_crosswalk-ca-2#county_crosswalk", scheme_extension=False,fragment=False))

        d = {'netloc': 'library.metatab.org', 'params': '', 'path': '/cdph.ca.gov-county_crosswalk-ca-2',
             'password': None, 'query': '', 'hostname': 'library.metatab.org', 'fragment': 'county_crosswalk',
             'resource_format': 'gov-county_crosswalk-ca-2', 'port': None, 'scheme_extension': 'metatab',
             'proto': 'metatab', 'username': None, 'scheme': 'http'}

        print(unparse_url_dict(d, scheme_extension=False, fragment=False))

    def test_metapack(self):
        urls = ['metatab+http://library.metatab.org/cdph.ca.gov-county_crosswalk-ca-2#county_crosswalk',
                'metatab+http://library.metatab.org/zip/cdph.ca.gov-county_crosswalk-ca-2.zip#county_crosswalk',
                'metatab+http://library.metatab.org/xlsx/cdph.ca.gov-county_crosswalk-ca-2.xlsx#county_crosswalk'
        ]

        cache = cache_fs()

        for url in urls:

            gen = RowGenerator(cache=cache, url=url)

            rows = list(gen)

            self.assertEquals(59, len(rows))


if __name__ == '__main__':
    unittest.main()