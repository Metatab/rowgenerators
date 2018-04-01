# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

from __future__ import print_function

import sys

from tabulate import tabulate

from rowgenerators.source import Source
from rowgenerators.exceptions import SourceError, TextEncodingError
from rowgenerators.appurl.url import Url
from rowgenerators.appurl.enumerate import enumerate_contents
from tableintuit import RowIntuiter
from itertools import islice

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

def run_row_intuit(path, cache):

    for encoding in ('ascii', 'utf8', 'latin1'):
        try:
            rows = list(islice(Source(url=path, encoding=encoding, cache=cache), 5000))
            return encoding, RowIntuiter().run(rows)
        except TextEncodingError:
            pass

    raise Exception('Failed to convert with any encoding')

def rowgen():
    import argparse

    parser = argparse.ArgumentParser(
        prog='rowgen',
        description='Return CSV rows of data from a rowgenerator URL')

    parser.add_argument('-H', '--head', default=False, action='store_true',
                        help='Display only the first 20 lines, in tabular format')

    parser.add_argument('-e', '--encoding',
                        help='Force the encoding')

    parser.add_argument('-f', '--format',
                        help="Force the file format. Typical values are 'csv', 'xls', 'xlsx' ")

    parser.add_argument('-u', '--urlfiletype',
                        help="Force the type of the file downloaded from the url. Equivalent to changing the file extension ")

    parser.add_argument('-s', '--start',
                        help='Line number where data starts')

    parser.add_argument('-d', '--headers', default=None, action='store_true',
                        help="Comma seperated list of header line numebrs")

    parser.add_argument('-E', '--enumerate', default=None, action='store_true',
                        help="Download the URL and enumerate it's contents as URLs")

    parser.add_argument('-i', '--intuit', default=None, action='store_true',
                        help="Intuit headers, start lines, etc")

    parser.add_argument('-I', '--info', default=None, action='store_true',
                        help="Print information about the url")

    parser.add_argument('-T', '--table', action='store_true',
                        help="When generating rows, output 20 rows in a table form. Otherwise, output CSV")

    parser.add_argument('url')

    cache = get_cache()

    args = parser.parse_args(sys.argv[1:])

    ss = Url(url=args.url, target_format=args.format, encoding=args.encoding, resource_format=args.urlfiletype)


    if args.info:
        prt(tabulate(ss.dict.items()))
        sys.exit(0)

    if args.enumerate:
        contents = list(enumerate_contents(ss, cache_fs=cache))

        for s in contents:
            print(s.rebuild_url())

    elif args.intuit:
        contents = list(enumerate_contents(ss, cache_fs=cache))
        for s in contents:

            try:
                encoding, ri = run_row_intuit(s.rebuild_url(),cache=cache)

                prt("{} headers={} start={} encoding={}".format(
                        s.rebuild_url(),
                        ','.join(str(e) for e in ri.header_lines),
                        ri.start_line,
                        encoding))
            except SourceError as e:
                warn("{}: {}".format(s.rebuild_url(), e))

    else:
        from rowgenerators import parse_app_url

        t =  parse_app_url(args.url, target_format=args.format, encoding=args.encoding,
                           resource_format=args.urlfiletype)\
                         .get_resource().get_target()

        rg = t.generator

        if args.table:
            print(tabulate(islice(rg,20)))
        else:

            import csv

            w = csv.writer(sys.stdout)

            w.writerows(rg)


