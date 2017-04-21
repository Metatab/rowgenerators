import unittest
from rowgenerators import get_cache, download_and_cache, SourceSpec

class TestIssues(unittest.TestCase):
    def test_windows_cache_paths(self):

        c = get_cache()

        print(c.getsyspath('/'))

        url = 'http://public.source.civicknowledge.com/example.com/sources/renter_cost.csv'
        dc = download_and_cache(SourceSpec(url), cache_fs=c)
        print(dc)

if __name__ == '__main__':
    unittest.main()
