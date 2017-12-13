# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""Pipes, pipe segments and piplines, for flowing data from sources to partitions.

"""

import textwrap
import os
import sys

class RowPipeError(Exception):
    pass



class CasterExceptionError(RowPipeError):

    def __init__(self,  function, field_header, value,  exc, exec_info, *args, **kwargs):

        exc_type, exc_obj, exc_tb = exec_info

        fname = exc_tb.tb_frame.f_code.co_filename
        linen = exc_tb.tb_lineno

        message = "Failed to cast column '{}' value='{}' in function='{}', file='{}', line='{}' : {} "\
            .format(field_header, value,  function,
                    fname, linen,
                    str(exc))

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)


class CastingError(RowPipeError):

    def __init__(self, type_target, field_header, value, message, *args, **kwargs):

        self.type_target = type_target
        self.field_header = field_header
        self.value = value

        message = "Failed to cast column '{}' value='{}' to '{}': {} "\
            .format(field_header, value, type_target, message)

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, textwrap.fill(message, 120), *args, **kwargs)


class TooManyCastingErrors(RowPipeError):

    def __init__(self, *args, errors = None, **kwargs):
        self.errors = errors if errors is not None else {}
        Exception.__init__(self, *args, **kwargs)



