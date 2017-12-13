# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""
CLI program for managing Metatab files
"""


import sys
from pkg_resources import iter_entry_points
from tabulate import tabulate

def appurl():

    import argparse
    parser = argparse.ArgumentParser(
        prog='appurl',
        description='app url configuration program',
       )

    g = parser.add_mutually_exclusive_group(required=True)

    g.add_argument('-l', '--list', action='store_true',
                   help="List all of the registered AppUrl handlers")

    args = parser.parse_args(sys.argv[1:])

    entries = []
    for ep in iter_entry_points('appurl.urls'):
        c = ep.load()
        entries.append([c.match_priority, ep.name, ep.module_name,  c.__name__, ])


    print(tabulate(sorted(entries), ['Priority', 'EP Name', 'Module', 'Class'] ))


if __name__ == "__main__":
    # execute only if run as a script
    appurl()