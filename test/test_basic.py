# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

import unittest
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

        def g():
            yield None

        print(get_generator([]))
        print(get_generator(g()))
        print(get_generator(parse_app_url('/foo/bar/file.csv')))

    def test_sources(self):
        from csv import DictReader

        with open(data_path('sources.csv')) as f:
            for e in DictReader(f):
                u = parse_app_url(e['url'])
                g = get_generator(u)

                print(e['name'], g)