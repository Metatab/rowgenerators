# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

""" """


class NotebookSource(Source):
    """Generate rows from an IPython Notebook. """

    def __init__(self, spec,  syspath, cache, working_dir):

        super(NotebookSource, self).__init__(spec, cache)

        self.sys_path = syspath
        if not exists(self.sys_path):
            raise SourceError("Notebook '{}' does not exist".format(self.sys_path))

        self.env = dict(
            (list(self.spec.generator_args.items()) if self.spec.generator_args else [])  +
            (list(self.spec.kwargs.items()) if self.spec.kwargs else [] )
            )

        assert 'METATAB_DOC' in self.env


    def start(self):
        pass

    def finish(self):
        pass

    def open(self):
        pass


    def __iter__(self):

        import pandas as pd

        env = self.execute()

        o = env[self.spec.target_segment]

        if isinstance(o, pd.DataFrame):
            r = PandasDataframeSource(self.spec,o,self.cache)

        else:
            raise Exception("NotebookSource can't handle type: '{}' ".format(type(o)))


        for row in r:
            yield row


    def execute(self):
        """Convert the notebook to a python script and execute it, returning the local context
        as a dict"""

        from nbconvert.exporters import get_exporter

        preprocessors = ['metatab.jupyter.preprocessors.PrepareScript']

        exporter = get_exporter('python')(preprocessors=preprocessors)

        (script, notebook) = exporter.from_filename(filename=self.sys_path)

        exec(compile(script.replace('# coding: utf-8', ''), 'script', 'exec'), self.env)


        return self.env

