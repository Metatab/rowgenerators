from __future__ import print_function

import unittest

import warnings
warnings.resetwarnings()
warnings.simplefilter("ignore")

class TestBasic(unittest.TestCase):


    def setUp(self):
        import warnings
        super().setUp()

        warnings.simplefilter('ignore')

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


    def test_expand_transform_1(self):
        from rowgenerators.rowpipe import Table
        from rowgenerators.rowpipe import RowProcessor
        from contexttimer import Timer
        from itertools import zip_longest

        def doubleit(v):
            return int(v) * 2

        env = {
            'doubleit': doubleit
        }

        t = Table('extable')
        t.add_column('id', datatype='int')
        t.add_column('b', datatype='int')
        t.add_column('v1', datatype='int',   transform='^row.a')
        t.add_column('v2', datatype='int',   transform='row.v1;doubleit')
        t.add_column('v3', datatype='int',   transform='^row.a;doubleit')

        for c in t:
            print('---',c)
            for i, tr in enumerate(c.expanded_transform):
                print('   ',i, len(list(tr)), list(tr))


        headers  = ['stage'] + list(c.name for c in t)

        table = [[i] + [ tr.str(i) for tr in stage ] for i, stage in enumerate(t.stage_transforms)]

        from tabulate import tabulate

        print (tabulate(table, headers, tablefmt="rst"))

        class Source(object):

            headers = 'a b'.split()

            def __iter__(self):
                for i in range(N):
                    yield i, 2*i

        rp = RowProcessor(Source(), t, env=env, code_path='/tmp/rowgenerators/test_transform.py')

    def test_expand_transform_2(self):
        from rowgenerators.rowpipe import Table
        from rowgenerators.rowpipe import RowProcessor
        from contexttimer import Timer
        from itertools import zip_longest

        def doubleit(v):
            return int(v) * 2

        env = {
            'doubleit': doubleit
        }

        t = Table('extable')
        t.add_column('id', datatype='int')
        t.add_column('v4', datatype='float', transform='^row.a;doubleit;doubleit')
        t.add_column('v5', datatype='int',   transform='^row.a;doubleit|doubleit')
        t.add_column('v6', datatype='str', transform="^str('v6-string')")

        for c in t:
            print('---',c)
            for i, tr in enumerate(c.expanded_transform):
                print('   ',i, len(list(tr)), list(tr))

        headers  = ['stage'] + list(c.name for c in t)

        table = [[i] + [ tr.str(i) for tr in stage ] for i, stage in enumerate(t.stage_transforms)]

        from tabulate import tabulate

        print (tabulate(table, headers, tablefmt="rst"))

        class Source(object):

            headers = 'a b'.split()

            def __iter__(self):
                for i in range(N):
                    yield i, 2*i

        rp = RowProcessor(Source(), t, env=env, code_path='/tmp/rowgenerators/test_transform.py')



    # NOTE. This speed test is about 12x to 23x faster running in PyPy than CPython!
    def test_basic_transform(self):

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
        t.add_column('a',  datatype='int')
        t.add_column('v1', datatype='int',   transform='^row.a')
        t.add_column('v2', datatype='int',   transform='row.v1;doubleit')
        t.add_column('v3', datatype='int',   transform='^row.a;doubleit')
        t.add_column('v4', datatype='float', transform='^row.a;doubleit;doubleit')
        t.add_column('v5', datatype='int',   transform='^row.a;doubleit|doubleit')
        t.add_column('v6', datatype='float')

        N = 20000

        class Source(object):

            headers = 'a b'.split()

            def __iter__(self):
                for i in range(N):
                    yield i, 2*i

        rp = RowProcessor(Source(), t, env=env, code_path='/tmp/rowgenerators/test_transform.py')


        print("Code: ", rp.code_path)

        headers = rp.headers

        for row in rp:

            d = dict(zip(headers, row))

            self.assertEqual(d['a'], d['v1'], d)
            self.assertEqual(2 * d['a'], d['v2'], d)
            self.assertEqual(2 * d['a'], d['v3'], d)
            self.assertEqual(4 * d['a'], d['v4'], d)
            self.assertEqual(4 * d['a'], d['v5'], d)

        count = 0
        row_sum = 0
        with Timer() as t:
            for row in rp:
                count += 1

                row_sum += round(sum(row[:6]))


        self.assertEqual(2199890000, row_sum)

        print('Rate=', float(N) / t.elapsed)


    def test_init_transform(self):

        from rowgenerators.rowpipe import Table


        def expand_transform(code, datatype='int', valuetype=None):
            
            t = Table('foobar')
            c = t.add_column('c', datatype=datatype, valuetype=valuetype, transform=code)
            return c.expanded_transform

        print(expand_transform('^GeoidCensusTract|v.as_acs()'))
        print(expand_transform('v.as_acs()'))



    def test_many_transform(self):

        from rowgenerators.rowpipe import Table
        from rowgenerators.rowpipe import RowProcessor
        from contexttimer import Timer

        def doubleit(v):
            return int(v) * 2

        def printstuff(v, manager, accumulator):
            print(type(accumulator), accumulator)
            return v

        def accumulate(v, accumulator):
            from collections import deque

            if not 'deque' in accumulator:
                accumulator['deque'] = deque([0], 3)

            accumulator['deque'].append(v)

            return sum(accumulator['deque'])


        def addtwo(x):
            return x+2

        env = {
            'doubleit': doubleit,
            'printstuff': printstuff,
            'accumulate': accumulate,
            'addtwo': addtwo
        }

        transforms = [
            ('',45),
            ('^row.a', 45),
            ('^row.a;doubleit', 90),
            ('^row.a;doubleit;doubleit', 180),
            ('^row.a;doubleit|doubleit', 180),
            ('^row.a;row.c*2|doubleit', 180),
            ('^row.a;row.c/3|doubleit', 24),
            ('doubleit', 90),
            ('doubleit;doubleit', 180),
            ('doubleit|doubleit', 180),
            ('row.c*2|doubleit', 180),
            ('row.c/3|doubleit', 24),
            ('accumulate', 109),
            ('manager.factor_a*row.c', 450),
            ('addtwo(row.c)', 65),
        ]

        N = 10

        class Source(object):
            headers = 'a'.split()
            def __iter__(self):
                for i in range(N):
                    yield (i,)


        class Manager(object):
            factor_a = 10

        for i, (tr, final_sum) in enumerate(transforms):
            t = Table('foobar')
            t.add_column('c', datatype='int', transform=tr)
            rp = RowProcessor(Source(), t, env=env,
                              manager=Manager(),
                              code_path='/tmp/rowgenerators/test_many_transform_{}.py'.format(i))

            row_sum = 0
            with Timer() as t:
                for row in rp:
                    row_sum += sum(row)

            self.assertEqual(final_sum, row_sum)

if __name__ == '__main__':
    unittest.main()
