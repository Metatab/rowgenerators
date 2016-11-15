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
    tmp = fsopendir(tempfile.gettempdir())
    return tmp.makeopendir('rowgenerator', recursive = True)


class BasicTests(unittest.TestCase):

    def test_source_spec_url(self):
        from rowgenerators import SourceSpec

        ss = SourceSpec(url='http://foobar.com/a/b.csv')
        self.assertIsNone(ss.file)
        self.assertIsNone(ss.sheet)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a')
        self.assertEqual('a',ss.file)
        self.assertIsNone(ss.sheet)

        ss = SourceSpec(url='http://foobar.com/a/b.zip#a;b')
        self.assertEqual('a',ss.file)
        self.assertEqual('b',ss.sheet)

    def test_run_csv(self):
        from rowgenerators import CsvSource, SourceSpec
        from rowgenerators.fetch import get_source

        cache = cache_fs()

        for sd in sources():
            # Don't have the column map yet.
            if sd['name'] in ('simple_fixed'):
                continue

            gen = SourceSpec(**sd).get_generator(cache)

            print gen.spec.name, gen.__class__.__name__, len(list(gen))
            print gen.headers

    def test_example(self):

        from rowgenerators import SourceSpec

        ss = SourceSpec(url='http://public.source.civicknowledge.com/example.com/basics/integers.csv')

        for row in ss.get_generator():
            print row


if __name__ == '__main__':
    unittest.main()
