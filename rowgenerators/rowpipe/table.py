# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
Tables and columns

"""

from rowgenerators.exceptions import  SchemaError
from rowgenerators.table import Column as RGColumn
from rowgenerators.table import Table as RGTable
from rowgenerators.valuetype import resolve_value_type

from rowgenerators.exceptions import ConfigurationError
from itertools import zip_longest

class Table(RGTable):

    def add_column(self, name, datatype=None, valuetype=None, transform=None, width=None):
        c = Column(name, datatype, width, valuetype, transform)
        self.columns.append(c)
        return c

    @property
    def stage_transforms(self):
        """Expanded transforms, organized as stages. Each entry is a list of one stage of transforms for all columns.
        Each stage has an entry for every colum, even when no transform is specified for that column, either
        setting the datatype for the first stage, or a passthrough function for any others."""

        stages = list(zip_longest(*[c.expanded_transform for c in self]))

        new_stages = []

        columns = list(self)

        for i, stage in enumerate(stages):
            new_stage = []
            for j, tr in enumerate(stage):
                if tr:
                    new_stage.append(tr)
                elif i == 0:
                    new_stage.append(TransformSegment(datatype=columns[j].valuetype or columns[j].datatype,
                                                      column=columns[j]))
                else:
                    new_stage.append(TransformSegment(transforms=['v'], column=columns[j]))


            new_stages.append(new_stage)

        return new_stages


    def __str__(self):
        from tabulate import tabulate
        headers = 'name datatype valuetype transform width'.split()
        rows = [(c.name, c.datatype.__name__, c.valuetype.__name__, c.transform, c.width) for c in self.columns]

        return ('Table: {}\n'.format(self.name)) + tabulate(rows, headers)

class TransformSegment(object):

    def __init__(self,  init=None, datatype=None, transforms=None, exception=None, column=None):
        self.init = init
        self.transforms = transforms or []
        self.datatype = datatype
        self.exception = exception
        self.column = column

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def __iter__(self):
        """Iterate the sequence of parts, including the init, datatype and transforms"""

        if self.init:
            yield self.init

        if self.datatype:
            yield self.datatype

        yield from self.transforms

    def __repr__(self):
        fields = []
        for f in 'init datatype transforms exception'.split():
            if self[f]:
                if f == 'datatype':
                    fields.append('{}={}'.format(f,self[f].__name__))
                else:
                    fields.append('{}={}'.format(f,self[f]))

        return "<Transform {} {} >".format(self.column.name, ' '.join(fields))


    def str(self,stage_n):

        return '|'.join(e.__name__ if isinstance(e, type) else e for e in list(self))

        if stage_n == 0:
            return self.init if self.init else self.datatype.__name__
        else:
            return '|'.join(list(self))


class Column(RGColumn):


    def __init__(self, name, datatype=None, width=None, valuetype=None, transform=None):

        if valuetype is not None and datatype is None:
            self.valuetype = resolve_value_type(valuetype)

        elif datatype is not None:
            self.valuetype = resolve_value_type(datatype)
        else:
            self.valuetype = None

        if self.valuetype is None:
            raise SchemaError("Could not resovle type for for column '{}' datatype='{}' and valuetype='{}' "
                              .format(name, datatype, valuetype))

        self.transform = transform

        if width is not None and width != '':
            width = int(width)

        super().__init__(name, self.valuetype.python_type(), width)


    def __repr__(self):
        return "<Column {name} dt={datatype} vt={valuetype} {transform}>"\
                .format(name=self.name, datatype=self.datatype.__name__, valuetype=self.valuetype.__name__,
                        transform=self.transform)

    @property
    def dict(self):
        return dict(
            name=self.name,
            datatype=self.datatype,
            valuetype=self.valuetype,
            transform=self.transform
        )

    @property
    def expanded_transform(self):
        """Expands the transform string into segments """

        transform = self.transform.rstrip('|') if  self.transform else ''

        segments = [TransformSegment(column=self)]


        for i, seg_str in enumerate(transform.split(';')):  # ';' seperates pipe stages

            pipes = seg_str.split('|')  # seperate pipes in each stage.

            d = TransformSegment(column=self)

            for pipe in pipes:

                if not pipe.strip():
                    continue

                if pipe[0] == '^':  # First, the initializer
                    if segments[0].init:
                        raise ConfigurationError('Can only have one initializer in a pipeline segment')
                    if i != 0:
                        raise ConfigurationError('Can only have an initializer in the first pipeline segment')
                    segments[0].init = pipe[1:] # initializers only go on the first segment

                elif pipe[0] == '!':  # Exception Handler
                    d.exception = pipe[1:]
                else:  # Assume before the datatype
                    d['transforms'].append(pipe.strip())

            if d['transforms'] or d.exception:
                segments.append(d)


        if not segments[0].init:
            segments[0].init = self.valuetype or self.datatype


        return segments