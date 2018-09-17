# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

from __future__ import print_function

import sys

from tabulate import tabulate

#Change the row cache name
from rowgenerators.util import  get_cache


def prt(*args):
    print(*args)

def warn( *args):
    print('WARN:',*args)

def err(*args):
    import sys
    print("ERROR:", *args)
    sys.exit(1)


def valuetypes():
    from rowgenerators.valuetype import value_types
    import argparse

    parser = argparse.ArgumentParser(
        prog='rowgen',
        description='Display information about rowgenerator valuetypes')

    parser.add_argument('-l', '--list', default=False, action='store_true',
                        help='List all of the valuetypes')

    cache = get_cache()

    args = parser.parse_args(sys.argv[1:])

    rows = [ (k,v.__name__, v.__doc__) for k,v in value_types.items() ]

    print(tabulate(sorted(rows), headers='Code Class Description'.split()))



