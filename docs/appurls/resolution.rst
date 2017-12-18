Resolving and Extending Urls
============================

The primary interface is :py:func:`appurls.parse_url``, which will find and construct
a :py:class:`appurl.url.Url` for a string. The function will select a Url class
using two selection criteria. The first is the ``appurl.urls`` entry
point. Here is the entrypoint configuration for the ``appurl`` package:


.. code-block:: python

      entry_points = {
        'appurl.urls' : [ "\* = appurl.url:Url",
            #
            "http: = appurl.web.web:WebUrl",
            "https: = appurl.web.web:WebUrl",
            "s3: = appurl.web.s3:S3Url",
            "socrata+ = appurl.web.socrata:SocrataUrl",
            #
            # Archive Urls
            ".zip = appurl.archive.zip:ZipUrl",
            #
            # File Urls
            ".csv = appurl.file.csv:CsvFileUrl",
            "file: = appurl.file.file:FileUrl",
        ]
    }



The key of each configuration like is a string the indicate the first
round of matching, and the value is the class to use for that matcher.
The match strings are:

-  'proto:' The URL protocol, which is based on either the URL scheme or
   scheme extension.
-  '.format' The target file format
-  'schemeext+' The URL scheme extension.
-  '\*' Any URL.

Because these configurations are in the entry pointy, you can extend the
AppUrls by including these entry points in Python packages.

The ``rowgenerators.appurls.parse_url`` collects all of the URL classes that pass the
initial matchers and sorts them by the ``Url.match_priority`` class
property. Then, it iterates through the matched classes in priority
order, calling the ``Url.match`` method. The function contructs and
returns the first match.