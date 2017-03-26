#! /usr/bin/env python -u

from string import ascii_uppercase
import sys
import csv
from os import getenv, environ
import json
import select

w = csv.writer(sys.stdout)

w.writerow(['type','k','v'])


for k in sys.argv:
    w.writerow(['arg', k, ''])

for k, v in environ.items():
    w.writerow(['env',k,v])

for k, v in json.loads(environ['PROPERTIES']).items():
    w.writerow(['prop', k, v])
