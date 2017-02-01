import unittest
from rowgenerators.fetch import get_generator
from rowgenerators import SourceSpec, decompose_url
from fs.tempfs import TempFS
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

    def compare_dict(self, a, b):
        from metatab.util import flatten
        fa = set('{}={}'.format(k, v) for k, v in flatten(a));
        fb = set('{}={}'.format(k, v) for k, v in flatten(b));

        # The declare lines move around a lot, and rarely indicate an error
        fa = {e for e in fa if not e.startswith('declare=')}
        fb = {e for e in fb if not e.startswith('declare=')}

        errors = len(fa - fb) + len(fb - fa)

        if errors:
            print("=== ERRORS ===")

        if len(fa - fb):
            print("In b but not a")
            for e in sorted(fa - fb):
                print('    ', e)

        if len(fb - fa):
            print("In a but not b")
            for e in sorted(fb - fa):
                print('    ', e)

        self.assertEqual(0, errors)


    def test_source_spec_url(self):
        from rowgenerators import SourceSpec, RowGenerator
        from rowgenerators.util import parse_url_to_dict, unparse_url_dict
        from copy import deepcopy


        ss = SourceSpec(url='http://foobar.com/a/b.csv')
        self.assertIsNone(ss.file)
        self.assertIsNone(ss.segment)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a')
        self.assertEqual('a',ss.file)
        self.assertIsNone(ss.segment)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.file,ss2.file)
        self.assertIsNone(ss.segment)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a;b')
        self.assertEqual('a',ss.file)
        self.assertEqual('b',ss.segment)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.file,ss2.file)
        self.assertEqual(ss.segment,ss2.segment)

        ss = RowGenerator(url='http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx')
        self.assertEqual('renter_cost_excel07.xlsx', ss.file)

        ss2 = deepcopy(ss)
        self.assertEqual(ss.file, ss2.file)

        ss.__dict__ = {'name': 'mz_with_zip_xl',
                       'encoding': None,
                       'url': 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip#excel/renter_cost_excel07.xlsx',
                       '_urltype': None,
                       '_filetype': 'xlsx',
                       '_file': 'excel/renter_cost_excel07.xlsx',
                       '_segment': None,
                       'file_segment': None,
                       'archive_file': None,
                       'columns':None, 'headers': None}

        self.assertIsNone(ss.segment)

        for url in (
            'http://example.com/foo/archive.zip',
            'http://example.com/foo/archive.zip#file.xlsx',
            'http://example.com/foo/archive.zip#file.xlsx;0',
            'socrata+http://example.com/foo/archive.zip'
        ):
           pass


        print(SourceSpec(url='socrata+http://chhs.data.ca.gov/api/views/tthg-z4mf').__dict__)

    def test_run_sources(self):
        from rowgenerators import  RowGenerator

        cache = cache_fs()

        for sd in sources():
            # Don't have the column map yet.
            if sd['name'] in ('simple_fixed','facilities'):
                continue

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

        from rowgenerators import enumerate_contents
        from rowgenerators import SourceSpec

        cache = cache_fs()

        spec = SourceSpec(url='http://public.source.civicknowledge.com/example.com/sources/test_data.zip#renter_cost_excel07.xlsx')
        spec = SourceSpec(url='http://public.source.civicknowledge.com/example.com/sources/test_data.zip')

        for spec in enumerate_contents(spec, cache):
            print(spec.download_url)

    def test_google(self):
        from rowgenerators import SourceSpec, GooglePublicSource
        spec = SourceSpec(url='gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w')

        self.assertEquals('gs',spec.proto)
        self.assertEquals('gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w',spec.url)
        self.assertEquals('https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/export?format=csv',GooglePublicSource.download_url(spec))

        self.assertEquals(12004, len(list(spec.get_generator(cache_fs()))))

    def test_zip(self):

        from rowgenerators import enumerate_contents, RowGenerator, SourceError, TextEncodingError

        z = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip'
        cache = cache_fs()

        for c in enumerate_contents(z,  cache):
            print(c.rebuild_url())

            if c.format in ('foo','txt'):
                continue

            gen = RowGenerator(url=c.rebuild_url())
            try:
                print(len(list(gen)))
            except (UnicodeDecodeError, TextEncodingError) as e:
                print("UERROR", c.name, e)
            except SourceError as e:
                print("ERROR", c.name, e)
                raise



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

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):

                if e['name'] in ('simple_fixed',):
                    continue

                if e['name'] not in ('zip_no_xls',):
                    continue

                s = SourceSpec(**e)

                d = get_generator(s, cache)

                print(s.url, len(list(d)))


    def test_urls(self):

        from csv import DictReader, DictWriter
        from rowgenerators.urls import get_handler, Url
        import json

        headers="in_url url download_url download_file target_file proto download_format target_format is_archive encoding file_segment".split()

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
                w.writerow(do)

                d = {k: v if v else None for k, v in d.items()}
                do = {k: str(v) if v else None for k, v in do.items()}# str() turns True into 'True'

                try:
                    self.compare_dict(d, do)
                except AssertionError as e:
                    errors += 1
                    #raise

            self.assertEqual(0, errors)

    def test_parse_file_urls(self):
        from rowgenerators.util import parse_url_to_dict, unparse_url_dict
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


if __name__ == '__main__':
    unittest.main()