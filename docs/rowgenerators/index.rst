Row Generators
==============

Row generators are iterator objects that are created from the targets of AppUrls. They have special support
for row headers and can iterate through row data as  rows, dicts, or RowProxy objects. The `RowGenerator` class creates
internal iterators for these file types:

* CSV
* TSV
* Fixed width text files
* XLS
* XLSX
* Google spreadsheets
* Socrata datasets
* Shapefiles
* Pandas dataframes
* Python functions
* General programs


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
* `http://.../foo.zip#<file>;<worksheet>` `<file>` is a regular expression for a spreadsheet in the zip file, and `<worksheet`> is the name or number of the worksheet.

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
