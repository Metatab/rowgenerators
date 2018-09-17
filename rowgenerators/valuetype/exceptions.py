""" Value Type Exceptions

Functions for handling exceptions

"""


def clear_error(v):
    from rowgenerators.valuetype import FailedValue

    if isinstance(v, FailedValue):
        return None
    return v

def try_except( try_f, except_f):
    """Takes 2 closures and executes them in a try / except block """
    try:
        return try_f()
    except Exception as exception:
        return except_f(exception)

nan_value = float('nan')

def nan_is_none(v):
    import math

    try:
        if math.isnan(v):
            return None
        else:
            return v
    except (ValueError, TypeError):
        return v

def ignore(v):
    return None

def log_exception(exception, bundle):
    bundle.error(exception)
    return None

