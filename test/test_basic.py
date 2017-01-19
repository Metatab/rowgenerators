import unittest

from fs.opener import fsopendir

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
    import tempfile
    #tmp = fsopendir(tempfile.gettempdir())
    tmp = fsopendir('/tmp')
    return tmp.makeopendir('rowgenerator', recursive = True)


class BasicTests(unittest.TestCase):

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
            self.assertEqual(url,SourceSpec(url=url).url_str() )
            self.assertEqual(url,SourceSpec(url=url).dict['url'])
            self.assertEquals(2, len(SourceSpec(url=url).dict))


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
            print(spec.url_str())

    def test_google(self):
        from rowgenerators import SourceSpec, GooglePublicSource
        spec = SourceSpec(url='gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w')

        self.assertEquals('gs',spec.proto)
        self.assertEquals('gs://1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/',spec.url)
        self.assertEquals('https://docs.google.com/spreadsheets/d/1VGEkgXXmpWya7KLkrAPHp3BLGbXibxHqZvfn9zA800w/export?format=csv',GooglePublicSource.download_url(spec))

        self.assertEquals(12004, len(list(spec.get_generator(cache_fs()))))

    def test_zip(self):

        from rowgenerators import enumerate_contents, RowGenerator, SourceError

        z = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip'
        cache = cache_fs()

        for c in enumerate_contents(z,  cache):
            print(c.dict)
            gen = RowGenerator(**c.dict)
            try:
                print(len(list(gen)))
            except SourceError as e:
                print("ERROR", c.name, e)
            except UnicodeDecodeError as e:
                print("UERROR", c.name, e)

    def test_url_decompose(self):

        from rowgenerators import decompose_url
        from csv import DictReader
        with open(data_path('decomp_urls.csv')) as f:
            r = DictReader(f)
            for d in r:
                url = d['in_url']
                del d['in_url']
                d['is_archive'] = d['is_archive'] == 'True'
                d  = {k: v if v else None for k, v in d.items()}
                du = {k: v if v else None for k, v in decompose_url(url).items()}
                print(url)
                self.assertEquals(d, du )

    def test_d_and_c(self):
        from csv import DictReader
        from rowgenerators.fetch import download_and_cache
        from fs.opener import fsopendir
        from os.path import isfile

        cache = fsopendir('temp://', create_dir=True)

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):
                d = download_and_cache(e['url'], cache)

                self.assertTrue(isfile(d['sys_path']))

    def test_delayed_flo(self):
        from csv import DictReader
        from rowgenerators.fetch import get_generator
        from fs.opener import fsopendir
        from os.path import isfile
        from rowgenerators import SourceSpec

        cache = fsopendir('/tmp/delayedflo', create_dir=True)

        success = []

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):

                if e['name'] in ('simple_fixed',):
                    continue

                s = SourceSpec(**e)

                d = get_generator(s, cache)

                print(s.url, len(list(d)))


if __name__ == '__main__':
    unittest.main()
