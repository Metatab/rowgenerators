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

    def test_run_csv(self):
        from rowgenerators import CsvSource, SourceSpec
        from rowgenerators.fetch import get_source

        cache = cache_fs()

        print "Cache", cache

        for sd in sources():
            if sd['name'] in ('simple_fixed'):
                continue

            #print sd

            gen = SourceSpec(**sd).get_generator(cache)

            print gen.spec.name, gen.__class__.__name__
            print len(list(gen))
            print gen.headers

if __name__ == '__main__':
    unittest.main()
