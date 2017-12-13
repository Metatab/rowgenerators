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

class Table(RGTable):

    def add_column(self, name, datatype=None, valuetype=None, transform=None, width=None):
        self.columns.append(Column(name, datatype, width, valuetype, transform))

    def __str__(self):
        from tabulate import tabulate
        headers = 'name datatype valuetype transform width'.split()
        rows = [(c.name, c.datatype.__name__, c.valuetype.__name__, c.transform, c.width) for c in self.columns]

        return ('Table: {}\n'.format(self.name)) + tabulate(rows, headers)

    pass


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


    @property
    def expanded_transform(self):
        """Expands the transform string into segments """

        segments = Column._expand_transform_to_segments(self.transform)

        vt = self.valuetype if self.valuetype else self.datatype

        if segments:

            segments[0]['datatype'] = vt

            for s in segments:
                s['column'] = self

        else:

            segments = [Column.make_xform_seg(datatype=vt, column=self)]

        # If we want to add the find datatype cast to a transform.
        # segments.append(self.make_xform_seg(transforms=["cast_"+self.datatype], column=self))

        return segments

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

    @staticmethod
    def make_xform_seg(init_=None, datatype=None, transforms=None, exception=None, column=None):
        return {
            'init': init_,
            'transforms': transforms if transforms else [],
            'exception': exception,
            'datatype': datatype,
            'column': column
        }

    @staticmethod
    def _expand_transform_to_segments(transform):


        if not bool(transform):
            return []

        transform = transform.rstrip('|')

        segments = []

        for i, seg_str in enumerate(transform.split(';')):  # ';' seperates pipe stages
            pipes = seg_str.split('|')  # eperates pipes in each stage.

            d = Column.make_xform_seg()

            for pipe in pipes:

                if not pipe.strip():
                    continue

                if pipe[0] == '^':  # First, the initializer
                    if d['init']:
                        raise ConfigurationError('Can only have one initializer in a pipeline segment')
                    if i != 0:
                        raise ConfigurationError('Can only have an initializer in the first pipeline segment')
                    d['init'] = pipe[1:]
                elif pipe[0] == '!':  # Exception Handler
                    if d['exception']:
                        raise ConfigurationError('Can only have one exception handler in a pipeline segment')
                    d['exception'] = pipe[1:]
                else:  # Assume before the datatype
                    d['transforms'].append(pipe)

            segments.append(d)

        return segments




