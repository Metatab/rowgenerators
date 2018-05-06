# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

Built-in Transform functions.

"""


def info(v, row, row_n, i_s, i_d, header_s, header_d, scratch, errors, accumulator):
    """ Print information about a value, and return the value. Prints out these values:

    - row_n: The row number
    - header_d: Schema header
    - type: The python type of the value
    - value: The value of the row, truncated to 40 characters.

    :param v: The current value of the column
    :param row: A RowProxy object for the whiole row.
    :param row_n: The current row number.
    :param i_s: The numeric index of the source column
    :param i_d: The numeric index for the destination column
    :param header_s: The name of the source column
    :param header_d: The name of the destination column
    :param scratch: A dict that can be used for storing any values. Persists between rows.
    :param errors: A dict used to store error messages. Persists for all columns in a row, but not between rows.
    :param accumulator: A dict for use in accumulating values, such as computing aggregates.
    :return: The final value to be supplied for the column.
    """

    print("{}:{} {} {}".format(row_n, header_d, type(v), str(v)[:40]))

    return v
