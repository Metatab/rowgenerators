# -*- coding: utf-8 -*-
# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""
A Row proxy provides a dict interface to row data, without having to re-construct a dict for each row.

NOTW: Don't store and reuse RowProxy objects; the iterators return the same RowProxy object, linked
to a different row each time, but with the same header. If you store them in a list, all of them
will have the row data of the last row. Comvert them to dicts ( RowProxy.dict ) before putting them into lists.

"""



class RowProxy(object):
    '''
    A dict-like accessor for rows which holds a constant header for the keys. Allows for faster access than
    constructing a dict, and also provides attribute access

    >>> header = list('abcde')
    >>> rp = RowProxy(header)
    >>> for i in range(10):
    >>>     row = [ j for j in range(len(header)]
    >>>     rp.set_row(row)
    >>>     print rp['c']

    '''

    def __init__(self, keys):

        self.__keys = keys
        self.__row = [None] * len(keys)
        self.__pos_map = {e: i for i, e in enumerate(keys)}
        self.__initialized = True

    @property
    def row(self):
        return object.__getattribute__(self, '_RowProxy__row')

    def set_row(self, v):
        object.__setattr__(self, '_RowProxy__row', v)
        return self

    @property
    def headers(self):
        return self.__getattribute__('_RowProxy__keys')

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.__row[key] = value
        else:
            self.__row[self.__pos_map[key]] = value

    def __getitem__(self, key):

            if isinstance(key, int):
                try:
                    return self.__row[key]
                except IndexError:
                    raise KeyError("Failed to get value for integer key '{}' in row {} ".format(key, self.__row))
            else:
                try:
                    return self.__row[self.__pos_map[key]]
                except IndexError:
                    raise IndexError("Failed to get value for non-int key '{}', resolved to position {} "
                                     .format(key, self.__pos_map[key]))
                except KeyError:
                    raise KeyError("Failed to get value for non-int key '{}' in row {} ".format(key, self.__row))

    def __setattr__(self, key, value):

        if '_RowProxy__initialized' not in self.__dict__:
            return object.__setattr__(self, key, value)

        else:
            self.__row[self.__pos_map[key]] = value

    def __getattr__(self, key):
        try:
            return self.__row[self.__pos_map[key]]
        except KeyError:
            raise KeyError("Failed to find key '{}'; has {}".format(key, self.__keys))

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.__keys)

    def __len__(self):
        return len(self.__keys)

    @property
    def dict(self):
        return dict(zip(self.__keys, self.__row))


    def copy(self):
        return type(self)(self.__keys).set_row(list(self.row))

    def keys(self):
        return self.__keys

    def values(self):
        return self.__row

    def items(self):
        return zip(self.__keys, self.__row)

    # The final two methods aren't required, but nice for demo purposes:
    def __str__(self):
        """ Returns simple dict representation of the mapping. """
        return str(self.dict)

    def __repr__(self):
        return self.dict.__repr__()


class GeoRowProxy(RowProxy):

    @property
    def __geo_interface__(self):
        from shapely.wkt import loads

        g = loads(self.geometry)
        gi = g.__geo_interface__

        d = dict(self)
        del d['geometry']

        gi['properties'] = d

        return gi

