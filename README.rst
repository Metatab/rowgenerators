Row Generators
==============

Python classes for generating rows from a variety of file formats


Simplest access with a URL

.. code-block:: python

    ss = SourceSpec(url='http://public.source.civicknowledge.com/example.com/basics/integers.csv')

    for row in ss.get_generator():
        print row


Use URL fragments to access a file in a ZIP archive.

.. code-block:: python

    SourceSpec(url='http://.../test_data.zip#simple-example.csv')


Set the encoding for the file:

.. code-block:: python

    SourceSpec(encoding='utf-8',
               url='http://.../test_data.zip#simple-example.csv')