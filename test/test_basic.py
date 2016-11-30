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
                       'file': 'excel/renter_cost_excel07.xlsx',
                       'segment': None,
                       'columns':None, 'headers': None}

        self.assertIsNone(ss.segment)

    def test_run_sources(self):
        from rowgenerators import  RowGenerator

        cache = cache_fs()

        for sd in sources():
            # Don't have the column map yet.
            if sd['name'] in ('simple_fixed',):
                continue

            gen = RowGenerator(cache=cache, **sd)

            self.assertEquals(int(sd['n_rows']), len(list(gen)))

    def test_example(self):

        from rowgenerators import SourceSpec

        ss = SourceSpec(url='http://public.source.civicknowledge.com/example.com/basics/integers.csv')

        for row in ss.get_generator():
            print row


    def test_inspect(self):
        from rowgenerators.fetch import inspect
        from rowgenerators import SourceSpec

        cache = cache_fs()

        url = 'http://public.source.civicknowledge.com/example.com/sources/test_data.zip'

        ss = SourceSpec(url=url)

        while True:

            specs = inspect(ss, cache)

            if not specs:
                break

            for i, e in enumerate(specs):
                print i, e.url_str()

            i = raw_input('Select: ')

            ss = specs[int(i)]


if __name__ == '__main__':
    unittest.main()
