# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """

from os.path import normpath, join, exists
from os import environ
import json

from rowgenerators.exceptions import SourceError
from rowgenerators.source import Source

class ProgramSource(Source):
    """Generate rows from a program. Takes kwargs from the spec to pass into the program. """

    def __init__(self, ref, cache=None, working_dir=None, env = None, **kwargs):

        super().__init__(ref, cache, working_dir, **kwargs)

        import platform

        if platform.system() == 'Windows':
            raise NotImplementedError("Program sources aren't working on Windows")

        assert working_dir

        self.program = normpath(join(working_dir, self.ref.path))

        if not exists(self.program):
            raise SourceError("Program '{}' does not exist".format(self.program))

        self.options = []

        self.properties = {}

        self.env = {}

        # Expand the generator args and kwargs into parameters for the program,
        # which may be command line options, env vars, or a json encoded dict in the PROPERTIES
        # envvar.
        for k, v in (env or {}).items():
            if k.startswith('--'):
                # Long options
                self.options.append("{} {}".format(k,v))
            elif k.startswith('-'):
                # Short options
                self.options.append("{} {}".format(k, v))
            elif k == k.upper():
                #ENV vars
                self.env[k] = v
            else:
                # Normal properties, passed in as JSON and as an ENV
                self.properties[k] = v

        self.env['PROPERTIES'] = json.dumps(self.properties)

        # Make sure that sys.stdout is always UTF*. It can end up US_ASCI otherwise.
        self.env['PYTHONIOENCODING']='utf-8:replace'

    def start(self):
        pass

    def finish(self):
        pass

    def open(self):
        pass

    def __iter__(self):
        import csv
        import subprocess
        from io import TextIOWrapper

        import sys


        if self.program.endswith('.py'):
            # If it is a python program, it's really nice, possibly required,
            # that the program be run with the same interpreter as is running this program.
            #
            # The -u option makes output unbuffered.  http://stackoverflow.com/a/17701672
            prog = [sys.executable, '-u', self.program]
        else:
            prog = [self.program]


        p = subprocess.Popen(prog + self.options,
                             stdout=subprocess.PIPE,
                             bufsize=1,
                             env=self.env)

        yield from csv.reader(TextIOWrapper(p.stdout, encoding='utf8', errors='replace'))
