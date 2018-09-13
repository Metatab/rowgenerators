"""Value Types

The value converters can recognize, parse, normalize and transform common codes, such as FIPS, ANSI and census codes.

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of
the Revised BSD License, included in this distribution as LICENSE.txt

"""


from .types import *
from .core import *
from .dimensions import *
from .errors import *
from .measures import *
from .other import *
from .times import *
from .geo import *

value_types = {
    "int": IntMeasure,
    "long": LongMeasure,
    "float": FloatMeasure,
    "text": TextDimension,
    "str": StrDimension,
    "unknown": StrDimension,
    "unicode": TextDimension,
    "geometry": ShapeValue,
}

value_types.update(geo_value_types)
value_types.update(times_value_types)
value_types.update(dimension_value_types)
value_types.update(error_value_types)
value_types.update(measure_value_types)
value_types.update(other_value_types)



@memoize
def resolve_value_type(vt_code):
    import sys

    vt_code = vt_code.strip('?')

    try:

        o = value_types[vt_code]
        o.vt_code = vt_code
        return o
    except KeyError:

        parts = vt_code.split('/')
        args = []
        while len(parts):

            args.append(parts.pop())

            try:
                base_vt_code = '/'.join(parts)
                o = value_types[base_vt_code]
                # Return a dynamic subclass that has the extra parameters built in
                cls = o.subclass(vt_code, '/'.join(args))

                # Stuff the class into the current module
                globals()[cls.__name__] = cls

                # And also in the same module as the base class
                setattr(sys.modules[o.__module__], cls.__name__, cls)

                # FIXME. Shouldn't need this, but codegen.py seems to try to import
                # from the wrong module.
                setattr(sys.modules['rowgenerators.valuetype.core'], cls.__name__, cls)

                return cls

            except KeyError:
                pass
