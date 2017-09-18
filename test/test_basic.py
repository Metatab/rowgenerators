# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import unittest
from os.path import dirname
from appurl import parse_app_url
from rowgenerators.generator.csv import CsvSource
from rowgenerators import get_generator

def get_file(url_str):

    from appurl import  parse_app_url

    u = parse_app_url(url_str)

    return u.get_resource().get_target()

def data_path(v):
    from os.path import dirname, join
    d = dirname(__file__)
    return join(d, 'test_data', v)


def script_path(v=None):
    from os.path import dirname, join
    d = dirname(__file__)
    if v is not None:
        return join(d, 'scripts', v)
    else:
        return join(d, 'scripts')


def sources():
    import csv
    with open(data_path('sources.csv')) as f:
        r = csv.DictReader(f)
        return list(r)


class BasicTests(unittest.TestCase):

    def test_csv(self):

        us = 'http://public.source.civicknowledge.com/example.com/sources/simple-example-altnames.csv'

        self.assertEqual(10001, len(list(CsvSource(get_file(us)))))

        us = 'http://public.source.civicknowledge.com/example.com/sources/unicode-utf8.csv'

        self.assertEqual(53, len(list(CsvSource(get_file(us)))))

    def test_entrypoints(self):
        from rowgenerators.generator.iterator import IteratorSource
        from rowgenerators.generator.generator import GeneratorSource
        from rowgenerators.generator.csv import CsvSource

        us = 'http://public.source.civicknowledge.com/example.com/sources/unicode-utf8.csv'

        def g():
            yield None

        self.assertIsInstance(get_generator([]), IteratorSource)
        self.assertIsInstance(get_generator(g()), GeneratorSource)
        self.assertIsInstance(get_generator(parse_app_url(us).get_resource().get_target()), CsvSource)

    def test_sources(self):
        from csv import DictReader

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):

                if not e['url_class']:
                    print()
                    continue

                u = parse_app_url(e['url'])
                r = u.get_resource()
                t = r.get_target()

                g = get_generator(t)

                self.assertEquals(e['gen_class'], g.__class__.__name__)

                self.assertEquals(int(e['n_rows']), (len(list(g))))


    def test_geo(self):

        us='shape+http://s3.amazonaws.com/public.source.civicknowledge.com/sangis.org/Subregional_Areas_2010.zip'
        u=parse_app_url(us)

        r = u.get_resource()

        print(type(r), r)

        t = r.get_target()

        print(type(t), t)

        g = get_generator(t)

        print(type(g))

        print(g.columns)
        print(g.headers)

        self.assertEquals(42, len(list(g)))

    def test_program(self):

        u = parse_app_url(script_path('rowgen.py'))
        u.scheme_extension = 'program'

        env = {
            '--long-arg': 'a',
            '-s': 'a',
            'ENV_VAR':'a',
            'prop1':'a',
            'prop2':'a'
        }

        g = get_generator(u, working_dir=dirname(u.path), env=env)

        print(type(g))

        rows = {}

        for row in g.iter_rp:
            rows[row['type']+'-'+row['k']] = row.v
            print(row)

        self.assertEqual('a', rows['prop-prop1'])
        self.assertEqual('{"prop1": "a", "prop2": "a"}', rows['env-PROPERTIES'])

    def test_fixed(self):
        from itertools import islice

        from rowgenerators import Table

        t = Table()
        t.add_column('id',int,6)
        t.add_column('uuid', str, 34)
        t.add_column('int', int, 3)
        t.add_column('float', float, 14)

        print(str(t))

        parse = t.make_fw_row_parser()

        u = parse_app_url('fixed+file:/Volumes/Storage/Downloads/test_data/fixed/simple-example.txt')

        print(u.get_resource())
        print(u.get_resource().get_target())

        g = get_generator(u, table=t)

        print(type(g))

        for row in islice(g,10):
            print(row)


