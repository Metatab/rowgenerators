import unittest
from rowgenerators import get_cache

class TestIssues(unittest.TestCase):
    def test_windows_cache_paths(self):

        c = get_cache()

        print(c.getsyspath('/'))



if __name__ == '__main__':
    unittest.main()
