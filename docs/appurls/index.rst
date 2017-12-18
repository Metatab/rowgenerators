Application Urls
================

.. toctree::
    :maxdepth: 2

    usage
    file_urls
    web_urls
    archive_urls
    resolution

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


The module defines an ``entry_point``, so other modules can extend the types of
URLs can that can produced by :py:meth:`.parse_app_url`. For instance,
the `pandas-reporter <https://github.com/CivicKnowledge/pandas-reporter>`_
module extends :py:mod:`appurl` to access Census tables from Census
Reporter, using URLs such as ``censusreporter:B17001/140/05000US06073``

Typical use -- for downloading an archive and extracting a file from it -- is:

.. code-block:: python

    from appurl import  parse_app_url
    from os.path import exists

    url = parse_app_url("http://example.com/archive.zip#file.csv")

    resource_url = url.get_resource() # Download the .zip file

    target_path = resource_url.get_target() # Extract `file.csv` from the .zip

    assert(exists(target_path)) # The path to file.csv