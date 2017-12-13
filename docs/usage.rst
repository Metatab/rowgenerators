Basic Usage
***********

Creating an AppUrl
==================

There are two ways to create an AppUrl: parse a string, or instantiate a class.
If you are starting from a string,  and in particular, don't know what the Url class
should be, use :py:meth:`.parse_app_url`. If you do know what kind of Url you want
to generate, use the :py:class:`Url` subclass directly.

After creating a URL, the basic useage involve either manipulating it, or fetching it.
THere are two fetching methods, :py:meth:`Url.get_resource`, to download files from
the web, and :py:meth:`Url.get_target` to extract a file from an archive or other
container. When the operation is unnecessary, such as getting the resource for
a resource URL that has already been downloaded, the :py:meth:`Url.get_resource`
returns ``self``.

These AppUrls have these components in addition to standard URLS:

-  A scheme extension, which preceedes the scheme with a '+'
-  A target\_file, the first part of the URL fragment
-  A target\_segment, the second part of a URL fragement, delineated by
   a ';'

The ``scheme_extension`` specifies the protocol to use with a sthadard
web scheme, inspired by github URLs like
``git+http://github.com/example``. The ``target_file`` is usually the
file within an archive. It is interpreted as a regular expression. The
``target_segment`` may be either a name or a number, and is usually
interpreted as the name or number of a worksheet in a spreadsheet file.
Combining these extensions:

::

        ckan+http://example.com/dataset/archive.zip#excel.xlsx;worksheet

This url may indicate that to fetch a ZIP file from an CKAN server,
using the CKAN protocol, extract the ``excel.xls`` file from the ZIP
archive, and open the ``worksheet`` worksheet.

The URLs define a few important concepts:

-  resource\_url: the portion of the URl that defines only the resource
   to be access or downloaded. In the eample above, the resource url is
   'http://example.com/dataset/archive.zip'
-  resource\_file: The basename of the resource URL: \`archive.zip'
-  resource\_format: Usually, the extension of the resource\_file: 'zip'
-  target\_file: The name of the target\_file: 'excel.xlsx'
-  target\_format: The extension of the target\_file: 'xlsx'



Using AppUrls
=============

Typical use is:

.. code-block:: python

    from appurl import  parse_app_url

    url = parse_app_url("http://example.com/archive.zip#file.csv")

    resource_url = url.get_resource()

    target_path = resource_url.get_target()

The call to ``url.get_resource()`` will download the resource file and store it in the cache ,returning a
``File:`` url pointing to the downloaded file. If the file is an archive, the call to ``resource.get_target()``
will extract the target file from the archive. If it is not an archive, it just returns the resource url. The final
result is that ``target_path`` is a Url pointing to a file in the filesystem.


Parsing Strings
===============

.. autofunction:: appurl.parse_app_url

The URL Base Class
==================

.. autoclass:: appurl.Url
    :members:


