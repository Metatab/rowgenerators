Transforms
==========

Transforms are series of functions that can be applied to columns, to perform manipulations on the columns values while a table is
being iterated. The transforms are usually simple functions, with some special argument names defined; when iterating, values are
automatically injected according to the argument names. Also, there are a few objects available as transforms, such as the entire
current row.

Each transform specification consists of names of functions or simple python expressions, which may include:

* Zero or one initializers, prefixed with a carat, '^'. 
* Zero or more stages, with no prefix, each of which can contain one or more segments.
* Zero or one exceptions handlers, prefixed with a bang, '!'. 

For instance, this transform specification: 

.. code-block::

    ^empty_is_null; lower; !handle_exception
    
Might initialize a column in a table with the value from the empty_is_null() function ( defined elsewhere ) , then call str.lower() on the result, and, if there is an exception, handle the exception with the handle_exception() function. 

Each of the segments of a transform can be an expression or a function. Functions can be among those defined in `rowgenerators.valuetype`, or a function that is added to the environment passed into the RowGenerator object when it is constructed


In this example, we have a source data that consists of the columns `a` and `b`, each of which is the first 10 integers. This data is fed into the table `foobar`, which has four columns defined. The first, names `id` will automatically get the row number. The second, `int_val_1` will be initialized form the `a` column of the dataset. The second `int_val_2` is initialized from the `b` column of the input, and then, in a second step, it is doubled. 

.. code-block:: python

        from rowgenerators.rowpipe import Table
        from rowgenerators.rowpipe import RowProcessor

        # Define a transform function

        def doubleit(v):
            return int(v) * 2

        # Construct a table, with transforms defined on some columns. 
                
        t = Table('foobar')
        t.add_column('id', datatype='int')
        t.add_column('val_1', datatype='int', transform='^row.a')
        t.add_column('val_2', datatype='int', transform='^row.b;doubleit')
        t.add_column('val_3', datatype='str', transform='^str(row.val_2)')

        # Add the function to the environment. 

        env = {
            'doubleit': doubleit
        }

        # Iterate over some data. 

        class Source(object):

            headers = 'a b'.split()

            def __iter__(self):
                for i in range(10):
                    yield i, i

        rp = RowProcessor(Source(), t, env=env)

        for row in rp:
            print(row)
        
In the output data:
* `id` will be the row number; this is due to special handling of columns named `id`
* `val_1` will be initialized to the same value as the `a` column of the input data
* `val_2` will be initialized to the value of the `b` column of the input, then doubled
# `val_3` will be set to a string version of `val_2`
    

When running a RowProcessor, the transforms are re-organized into "stages", with a number of stages equal to the number of stages of the longest transform on a column in the table. The first stage is the initializers ( '^' ); if an initializer is not specified, a default one is used. The result is a new row.  In the next stage, all of the second stages of each column transform are run, transforming the output row from the first stage into a new output row. This process continues for all of the output rows. 

The source data that is passed into the RowProcess may have a different structure than the output table of the RowProcess. The first stage -- the initializer stage -- will also transform the structure of the data, by assigning source columns to dest columns based on name, or using None for destination columns if there is no associated source column. 


For instance, the transform spec `^init; stage1; stage2; !ehandler` would set the final value in a similar fashion to this code:

.. code-block:: python

    try:
        v = init()
        v = stage1(v)
        v = stage2(v)
    except Exception as e:
        ehandler(e)
    
The function values `init`, `stage1`, `stage2` and `ehandler` must either be from the `rowgenerator` package, or be added to the row processors, environment, as with the `env` dict in the first example. 

The conceptual process for processing each row is: 

# Take a row from the source data
# Assign values from the source row into the destination table by matching names. Assign None to any destination column without an associated source column. If the first column is named 'id' and there is no associated source column, assign the row number. 
# Call the initializers for each column, and cast each column to a ValueType object with a type based on the datatype of the column. 
# For each remaining stage, start with the row from the previous stage and apply all of the transforms for this stage
# Repeat until all of the stages are run. 



Initializers
------------

The first stage of processing a row initializes the row from the source data.


Transforms
----------


Exceptions
----------




How It Works
------------

Consider this table definition:

.. code-block:: python

        t = Table('extable')
        t.add_column('id', datatype='int')
        t.add_column('b', datatype='int')
        t.add_column('v1', datatype='int',   transform='^row.a')
        t.add_column('v2', datatype='int',   transform='row.v1;doubleit')
        t.add_column('v3', datatype='int',   transform='^row.a;doubleit')

This defintion will result in three stages, with the transformation for each column, at each stage, shown in the table below. 

=======  ==========  ==========  ================  ==========  ================
  stage  id          b           v1                v2          v3
=======  ==========  ==========  ================  ==========  ================
      0  IntMeasure  IntMeasure  row.a|IntMeasure  IntMeasure  row.a|IntMeasure
      1  v           v           v                 row.v1      doubleit
      2  v           v           v                 doubleit    v
=======  ==========  ==========  ================  ==========  ================

The value 'v' in a cell indicates that the value from the previous stage is passed through. The value `IntMeasure` is a valuetype
object, which holds an integer.

The RowProcessor generates code for this table, with a function for each of the stages. Here is the first stage row function: 

.. code-block:: python


    def row_extable_0(row, row_n, errors, scratch, accumulator, pipe, manager, source):

        return [
            extable_id_0(row_n, None, 0, None, 'id', row, row_n, errors, scratch, accumulator, pipe, manager, source), # column id
            extable_b_0(row[1], 1, 1, 'b', 'b', row, row_n, errors, scratch, accumulator, pipe, manager, source), # column b
            extable_v1_0(None, None, 2, None, 'v1', row, row_n, errors, scratch, accumulator, pipe, manager, source), # column v1
            extable_v2_0(None, None, 3, None, 'v2', row, row_n, errors, scratch, accumulator, pipe, manager, source), # column v2
            extable_v3_0(None, None, 4, None, 'v3', row, row_n, errors, scratch, accumulator, pipe, manager, source), # column v3
        ]

The function takes an input row, along with some other management objects, and returns a row. The returned list has one entry for
each of the columns in the destination table. The first argument to each function is the value being passed in from the source
data. In this case, the source data only has two columns, 'a' and 'b'. The first entry, for the `id` column, is given a specialq
value, the row number. The second column is named `b`, the same name as in the source data, so it is given a value of the `b`
column in the source data. The remainder of the columns in the destination table have no counterpart in source table, so they have
values of `None`

This is the column function for the `id` column: 

.. code-block:: python


    def extable_id_0(v, i_s, i_d, header_s, header_d, row, row_n, errors, scratch, accumulator, pipe, manager, source):

        try:
            v = IntMeasure(v) # .../rowgenerators/rowpipe/codegen.py:345

        except Exception as exc:

            raise CasterExceptionError("extable_id_0",header_d, v, exc, sys.exc_info())

        return v

It just takes the input value, which was `row_n`, and casts it to an `IntMeasure`

The `v1` column has an initializer, so it is a bit different; it will take the `a` value from the source row and assign it to the `v1` column, then casts to IntMeasure

.. code-block:: python


    def extable_v1_0(v, i_s, i_d, header_s, header_d, row, row_n, errors, scratch, accumulator, pipe, manager, source):

        try:
            v = row.a # .../rowgenerators/rowpipe/codegen.py:548
            v = IntMeasure(v) # .../rowgenerators/rowpipe/codegen.py:348

        except Exception as exc:

            raise CasterExceptionError("extable_v1_0",header_d, v, exc, sys.exc_info())

        return v


