import unittest
from rowgenerators import get_generator
from rowgenerators import parse_app_url, get_cache

class TestIssues(unittest.TestCase):


    def x_test_pass_target_format(self):

        us = 'file:///Users/eric/Downloads/7908485365090507159.zip#VictimRecords.txt&target_format=csv'

        u = parse_app_url(us, target_format=None)

        print(u)
        r = u.get_resource()
        print(r)
        t = r.get_target()
        print(t)

        g = t.generator

        print(len(list(g)))



if __name__ == '__main__':
    unittest.main()
