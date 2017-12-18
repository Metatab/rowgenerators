from __future__ import print_function

import unittest


class TestBasic(unittest.TestCase):


    def test_table(self):

        from rowgenerators.rowpipe import Table

        t = Table('foobar')
        t.add_column('i1',datatype='int')
        t.add_column('i2', valuetype='int')
        t.add_column('i3', valuetype='measure/int')
        t.add_column('f1',datatype='float')
        t.add_column('f2', valuetype='float')
        t.add_column('f3', valuetype='measure/float')

        self.assertEqual(6, len(list(t)))

        for c in t:
            print(c)

    # NOTE. This speed test is about 12x to 23x faster running in PyPy than CPython!
    def test_transform(self):

        from rowgenerators.rowpipe import Table
        from rowgenerators.rowpipe import RowProcessor
        from contexttimer import Timer

        def doubleit(v):
            return int(v) * 2

        env = {
            'doubleit': doubleit
        }

        t = Table('foobar')
        t.add_column('id', datatype='int')
        t.add_column('other_id', datatype='int', transform='^row.a')
        t.add_column('i1', datatype='int', transform='^row.a;doubleit')
        t.add_column('f1', datatype='float', transform='^row.b;doubleit')
        t.add_column('i2', datatype='int', transform='^row.a')
        t.add_column('f2', datatype='float', transform='^row.b')

        N = 20000

        class Source(object):

            headers = 'a b'.split()

            def __iter__(self):
                for i in range(N):
                    yield i, 2*i

        rp = RowProcessor(Source(), t, env=env)

        count = 0
        row_sum = 0
        with Timer() as t:
            for row in rp:
                count += 1
                row_sum += sum(row)

        self.assertEquals(2199890000, row_sum)

        print('Rate=', float(N) / t.elapsed)

if __name__ == '__main__':
    unittest.main()
