import unittest
from rowgenerators import get_generator
from appurl import parse_app_url

class TestIssues(unittest.TestCase):
    def test_windows_cache_paths(self):

        c = get_cache()

        print(c.getsyspath('/'))

        url = 'http://public.source.civicknowledge.com/example.com/sources/renter_cost.csv'
        dc = download_and_cache(SourceSpec(url), cache_fs=c)
        print(dc)


    def test_zip_file(self):

        us = 'metapack+http://library.metatab.org/example.com-simple_example-2017-us-1.zip#metadata.csv'

        g = get_generator(us)

        print (list(g))

if __name__ == '__main__':
    unittest.main()
