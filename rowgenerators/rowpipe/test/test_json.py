from __future__ import print_function

import unittest

from rowgenerators.rowpipe.json import add_to_struct, VTEncoder

class TestJson(unittest.TestCase):


    def test_basic(self):
        import json

        d = {}

        add_to_struct(d, 'a', 1)
        add_to_struct(d, 'b', 2)
        add_to_struct(d, 'c.a', 1)
        add_to_struct(d, 'c.b', 1)
        add_to_struct(d, 'd[]', 1)
        add_to_struct(d, 'd[]', 2)
        add_to_struct(d, 'e[].a', 10)
        add_to_struct(d, 'e[].b', 11)
        add_to_struct(d, 'f[].a[]', 20)
        add_to_struct(d, 'f[].a[]', 21)
        add_to_struct(d, 'f[].b[]', 30)
        add_to_struct(d, 'f[].b[]', 31)
        add_to_struct(d, 'attr[].key', 'k1')
        add_to_struct(d, 'attr[-].value', 'v1')
        add_to_struct(d, 'attr[].key', 'k2')
        add_to_struct(d, 'attr[-].value', 'v2')

        print (json.dumps(d, indent=4))

    @unittest.skip('Has loacal path')
    def test_table(self):
        import json
        from metapack import open_package
        from itertools import islice

        u = '/Volumes/Storage/proj/virt/data-projects/client-boston-college/bc.edu-dataconv_poc/_packages/bc.edu-dataconv_poc-1/'
        pkg = open_package(u)
        r = pkg.resource('comments')

        json_headers = [ (c['pos'], c.get('json')) for c in r.columns()]

        for row in islice(r, None, 10):
            d = {}
            for pos, jh in json_headers:
                add_to_struct(d, jh, row[pos])

            print(json.dumps(d, indent=4, cls=VTEncoder))

if __name__ == '__main__':
    unittest.main()
