#! /usr/bin/env python -u

from string import ascii_uppercase
import sys
import csv
from os import getenv, environ
import json
import select

if __name__ == "__main__":

    try:
        w = csv.writer(sys.stdout)

        w.writerow(['row','type','k','v'])

        i=0

        for k in sys.argv:
            w.writerow([i,'arg', k, ''])
            i += 1

        for k, v in environ.items():
            w.writerow([i,'env',k,v])
            i += 1

        for k, v in json.loads(environ.get('PROPERTIES')).items():
            w.writerow([i, 'prop', k, v])
            i += 1
    except BrokenPipeError:
        # Parent exited before we wrote everything. This is almost guaranteed to
        # happen while building schemas with `metapack -s`
        pass

