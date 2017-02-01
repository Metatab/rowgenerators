# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

from __future__ import print_function

import sys
from rowgenerators._meta import __version__
from rowgenerators import RowGenerator, enumerate_contents, SourceSpec, get_cache
from tabulate import tabulate
from itertools import islice


#Change the row cache name
from rowgenerators.util import  get_cache, clean_cache

def prt(*args):
    print(*args)

def warn( *args):
    print('WARN:',*args)

def err(*args):
    import sys
    print("ERROR:", *args)
    sys.exit(1)


def rowgen():
    import argparse

    parser = argparse.ArgumentParser(
        prog='rowgen',
        description='Return CSV rows of data from a rowgenerator URL'.format(__version__))

    parser.add_argument('-H', '--head', default=False, action='store_true',
                        help='Display only the first 20 lines, in tabular format')

    parser.add_argument('-e', '--encoding', default=None, action='store_true',
                        help='Force the encoding')

    parser.add_argument('-f', '--format', default=None, action='store_true',
                        help="Force the file format. Typical values are 'csv', 'xls', 'xlsx' ")

    parser.add_argument('-u', '--urlfiletype', default=None, action='store_true',
                        help="Force the type of the file downloaded from the url. Equivalent to changing the file extension ")

    parser.add_argument('-s', '--start',
                        help='Line number where data starts')

    parser.add_argument('-d', '--headers', default=None, action='store_true',
                        help="Comma seperated list of header line numebrs")

    cache = get_cache()

    parser.add_argument('url')

    args = parser.parse_args(sys.argv[1:])

    ss = SourceSpec(url=args.url, format=args.format, encoding=args.encoding, urlfiletype=args.urlfiletype)

    for s in enumerate_contents(ss, cache_fs=cache):

        print(s.rebuild_url())

        rg = s.get_generator(cache=cache)

        print(tabulate(islice(rg,20)))

