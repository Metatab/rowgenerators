# Copyright (c) 2016 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE.txt

"""

Create row processor for generating JSON

The format for each line is a sequence of path elements, with a terminal at the end. For instance:

    a.b.c.t


Assume there is a path specification for each column in the table, and that the column name specification substituted
for a missing path specification.

A path element without a '[]' specifies a key to an object, and a path element with a '[]' specifies an array.
A path element with a '[.]' specifies that the value should be set on the same list item as the last path element.

* "a: v": Create Key a, set to v
* "a.b: v": Create Key a, set to new object. Create key b, set to v
* "a.b[]: v" Create key a, set to new object. Create key b, set to new list. Append v
* "a[].b: v" Create key a, set to new list. Create new object. Create key b, set to v

The "[-]" means to use the last element of the existing list.

Types of path elements:

    a: terminal, object key
    a.: nonterminal, object key and new object
    a[]: terminal, add to new or existing list
    a[].: nonterminal, new array with new object
    a[-].:nonterminal, last object in list

"""

import json

def parse_path(path):
    """Decompose a JSON structure path"""

    parts = []

    for e in path.split('.'):
        if e.endswith('[]'):
            k,_ = e.split('[')
            parts.append([k,'an', False])  # array, new
        elif e.endswith('[-]'):
            k, _ = e.split('[')
            parts.append([k, 'al', False]) # array, last
        else:
            k = e
            parts.append([k, 'o', False])  # object

    if parts:
        parts[-1][2] = True # The last item is the terminal

    return parts

def add_to_struct(s, path, v):
    """Add a value v into a complex data structure s at a given path. """
    o = s

    path_parts = parse_path(path)

    #print("----", path)

    for i,  (key, type, is_terminal) in enumerate(path_parts):

        if type == 'an' and not is_terminal:

            if key not in o:
                o[key] = []

            o[key].append({})

            o = o[key][-1]

        elif type == 'al' and not is_terminal:
            if key not in o:
                raise Exception("Expected list '{}' to exist in '{}' ".format(key, path) )

            o = o[key][-1]

        elif type == 'o' and not is_terminal:
            if key not in o:
                o[key] = {}
            o = o[key]
        elif type == 'an' and is_terminal:
            if key not in o:
                o[key] = []
            o[key].append(v)

        elif type == 'al' and is_terminal:
            print("E")

        elif type == 'o' and is_terminal:
            o[key] = v
        else:
            print("G ", type, is_terminal)

class VTEncoder(json.JSONEncoder):
     """A JSON object encoder that can handle dates and times. Just converts them to strings"""

     def default(self, obj):
         from rowgenerators.valuetype import DateTimeVT, DateVT, TimeVT

         if isinstance(obj, (DateTimeVT, DateVT, TimeVT) ):
             return str(obj)

         # Let the base class default method raise the TypeError
         try:
            return json.JSONEncoder.default(self, obj)
         except TypeError as e:
             raise TypeError(DateTimeVT,type(obj))