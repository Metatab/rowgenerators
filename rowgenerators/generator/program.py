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

    def __init__(self, url, working_dir):

        import platform

        if platform.system() == 'Windows':
            raise NotImplementedError("Program sources aren't working on Windows")


        assert working_dir

        self.program = normpath(join(working_dir, self.spec.url_parts.path))

        if not exists(self.program):
            raise SourceError("Program '{}' does not exist".format(self.program))

        self.args = dict(list(self.spec.generator_args.items() if self.spec.generator_args else [])+list(self.spec.kwargs.items()))

        self.options = []

        self.properties = {}

        self.env = dict(environ.items())

        # Expand the generator args and kwargs into parameters for the program,
        # which may be command line options, env vars, or a json encoded dict in the PROPERTIES
        # envvar.
        for k, v in self.args.items():
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
        import json

        # SHould probably give the child process the -u option,  http://stackoverflow.com/a/17701672
        p = subprocess.Popen([self.program] + self.options,
                        stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                        env = self.env)

        p.stdin.write(json.dumps(self.properties).encode('utf-8'))

        try:
            r = csv.reader(TextIOWrapper(p.stdout))
        except AttributeError:
            # For Python 2
            r = csv.reader(p.stdout)

        for row in r:
            yield row

