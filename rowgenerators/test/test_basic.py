# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import unittest
from os.path import dirname
import sys

from rowgenerators import get_generator, parse_app_url
from rowgenerators.generator.csv import CsvSource
from rowgenerators.test.support import get_file, data_path, script_path


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

    @unittest.skipIf(sys.platform.startswith("win"), "requires Windows")
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
        # SHould be self.assertEqual('{"prop1": "a", "prop2": "a"}', rows['env-PROPERTIES'])
        # but there are sorting issues, and it's a string, not an actual dict
        self.assertIn('"prop1": "a"', rows['env-PROPERTIES'])
        self.assertIn('"prop2": "a"', rows['env-PROPERTIES'])


    @unittest.skip('Has local path')
    def test_fixed(self):
        from itertools import islice

        from rowgenerators.table import  Table, Column

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

    def test_google(self):

        url = 'gs:1qjjtkMqpxtkDp3qZlkF7P8Tm8VtfIwiWW-OqJ2J91yE#2038675149'

        u = parse_app_url(url)

        wu = u.web_url
        print(type(wu), wu)

        r = u.get_resource()

        print(type(r), r.path)

        t = r.get_target()

        print(type(t), t.path)

        for r in t.generator:
            print(r)


    def test_RowGenerator(self):
        import warnings
        warnings.simplefilter("ignore")

        from rowgenerators import RowGenerator


        rg = RowGenerator('http://public.source.civicknowledge.com/example.com/sources/simple-example-altnames.csv')

        self.assertEqual(10001, len(list(rg)))

        df = rg.dataframe()

        self.assertEqual(10000, len(df))

        url = 'http://public.source.civicknowledge.com/example.com/sources/simple-example.foo#&target_format=csv'

        self.assertEqual(10001, len(list(RowGenerator(url))))

        url = 'http://public.source.civicknowledge.com/example.com/sources/simple-example.foo'

        self.assertEqual(10001, len(list(RowGenerator(url, target_format='csv' ))))
