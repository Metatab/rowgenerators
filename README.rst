Row Generators
==============

Note: This library is a hasily seperated part of a larger system, and isn't really packaged and documented
for external use.

Python classes for generating rows from a variety of file formats. The `RowGenerator` class creates internal
iterators for these file types:

* CSV
* TSV
* Fixed width text files
* XLS
* XLSX
* Google spreadsheets
* Socrata datasets
* Shapefiles

There are also internal iterators for other types that are not acessible from `RowGenerator`:

* Pandas dataframes
* ASPW cursors
* Ambry partitions


The `RowGenerator` constructor can take these configuration parameters: 

* `name` An optional name for the source
* `url` A Url reference to the file, or a local file system path
* `file` A reference to an internal file in a Zip archive. May a string, or a regular expression.
* `sheet` A reference to a worksheet in a spreadsheet. May be a string or a number
* `urltype` One of http, https, gs, socrata. Forces how the URL is interpreted. Only 'socrata' is really 
        needed
* `filetype` Forces the file type, which is usually taked from the file extension. May be any 
        typical `extension` string. 
* `urlfiletype` Like filetype, but for when the URL refers to a zip archive. 
* `encoding` The file encoding.
* `columns` A list or tuple of ColumnSpec objects, for FixedSource

The `url` can have a fragment to indicate which file to access in a zip file, which worksheet to use in a
spreadsheet, or both.

* `http://.../foo.zip#<file>`. `<file>` is a regular expression that matches a file in the archive. The first match is used
* `http://.../foo.xls#<worksheet>` `<worksheet>` is the name or number of a worksheet in a spreadsheet
* `http://.../foo.zip#<file>;<worksheet>` `<file>` is a regular expression for a spreadsheet in the zip file, and
`<worksheet`> is the name or number of the worksheet.

The `<file> fragement parameter sets the `file` parameter of `RowGenerator`, and `<worksheet>` sets the `segment`
parameter. Both can be set as parameters instead of in the URL.


The only value that is really necessary for the `urlfiletype` parameter is 'socrata' which indicates that the
URL should be interpreted as a Socrata site


Simple access with a URL

.. code-block:: python

    rg = RowGenerator(url='http://public.source.civicknowledge.com/example.com/basics/integers.csv')

    for row in rg:
        print row


Use URL fragments to access a file in a ZIP archive.

.. code-block:: python

    rg = RowGenerator(url='http://.../test_data.zip#simple-example.csv')


Set the encoding for the file:

.. code-block:: python

    rg = RowGenerator(encoding='utf-8',
               url='http://.../test_data.zip#simple-example.csv')

Application Urls
================


.. image:: https://travis-ci.org/Metatab/appurl.svg?branch=master
    :target: https://travis-ci.org/Metatab/appurl

Application Urls provide structure and operations on URLS where the file the
URL refers to can't, in general, simply be downloaded. For instance, you may
want to refer to a CSV file inside a ZIP archive, or a worksheet in an Excel
file. In conjunction with `Row Generators
<https://github.com/CivicKnowledge/rowgenerators>`_, Application Urls are often
used to refer to tabular data stored on data repositories. For instance:

-  Stored on the web: ``http://examples.com/file.csv``
-  Inside a zip file on the web: ``http://example.com/archive.zip#file.csv``
-  A worksheet in an Excel file: ``http://example.com/excel.xls#worksheet``
-  A worksheet in an Excel file in a ZIP Archive:
   ``http://example.com/archive.zip#excel.xls;worksheet``
-  An API: ``socrata+http://chhs.data.ca.gov/api/views/tthg-z4mf``


Install
*******


.. code-block:: bash

    $ pip install appurl

Documentation
*************

See the documentation at http://appurl.readthedocs.io/

Development Notes
*****************

Running tests
+++++++++++++

Run ``python setup.py tests`` to run normal development tests. You can also run ``tox``, which will
try to run the tests with python 3.4, 3.5 and 3.6, ignoring non-existent interpreters.


Development Testing with Docker
+++++++++++++++++++++++++++++++

Testing during development for other versions of Python is a bit of a pain, since you have
to install the alternate version, and Tox will run all of the tests, not just the one you want.

One way to deal with this is to install Docker locally, then run the docker test container
on the source directory. This is done automatically from the Makefile in appurl/tests


.. code-block:: bash

    $ cd ./docker
    $ make build # to create the container image
    $ make shell # to run bash the container

You now have a docker container where the /code directory is the appurl source dir.

Now, run tox to build the tox virtual environments, then enter the specific version you want to
run tests for and activate the virtual environment.

.. code-block:: bash

    # tox
    # cd .tox/py34
    # source bin/activate # Activate the python 3.4 virtual env
    # cd ../../
    # python setup.py test # Cause test deps to get installed
    #
    # python -munittest appurl.test.test_basic.BasicTests.test_url_classes  # Run one test





Row Data Pipeline
=================

The Rowpipe library manages row-oriented data transformers. Clients can create a RowProcessor() that has schema, composed of tables and columns, where each column cna have a "transform" that describes how to alter the data in the column.

.. code-block:: python

    from rowpipe.table import Table
    from rowpipe.processor import RowProcessor

    def doubleit(v):
        return int(v) * 2

    env = {
        'doubleit': doubleit
    }

    t = Table('foobar')
    t.add_column('id', datatype='int')
    t.add_column('other_id', datatype='int', transform='^row.a')
    t.add_column('i1', datatype='int', transform='^row.a;doubleit')
    t.add_column('f1', datatype='float', transform='^row.b;doubleit')
    t.add_column('i2', datatype='int', transform='^row.a')
    t.add_column('f2', datatype='float', transform='^row.b')


In this table definition, ``other_id`` and ``i2`` columns are  initialized to the valu of the ``a`` column in the input row,
The  ``i1`` column is initialized to the input row ``a`` column, then the ``doubleit`` function is called on the value. In the last step, all of the values are cast to the types specified in the ``datatype`` column.

The RowProcessor is then run using this table definition, and an input generator:

.. code-block:: python

    class Source(object):

        headers = 'a b'.split()

        def __iter__(self):
            for i in range(N):
                yield i, 2*i

    rp = RowProcessor(Source(), t, env=env)



Then, ``rp`` is a generator that returns ``RowProxy`` objects, which can be indexed as integers or by clolumn number:


.. code-block:: python

    for row in rp:
        v1 = row['f1']
        v2 = row[3]

The RowProcessor creates Python code files and executes them.

Transforms can have several steps, seperated by ';'. The first, prefixes with a '^', initializes the value for the rest of the transforms. A transform that is prefixes with a '!' is executed on exceptions.  Transform functions can have a variable signature; the tranform processor matches argument names. Valid argument names are:

- row. A rowProxy object for the input row. Allows access to any input row value
- row_n. Row number.
- scratch. A dict for temporary storage
- errors. A defaultdict(set) for storing error reports for columns. Keys are column names
- accumulator. A dict for accumulating value, such as sums.
- pipe. Unused
- bundle. Unused
- source. Reference to the input generator that is generating rows
- v . The input row value
- header_s. The header for the column in the input row.
- i_s. The index of the column in the input row
- header_d. The header for the column in the output row.
- i_d.  The index of the column in the output row

... and there is a whole lot more. This documentation is woefully incomplete ...

Notes
-----

This repo still contains old code for Row Pipelines, which are in the ``pipeline.py`` file. These components can be combined to performd defined operations on rows, such as skipping rows based on a predicate, altering the number of rows, returning on ly the head or tail, etc. The code is not currently used ot tested.



