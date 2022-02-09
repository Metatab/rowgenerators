#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from setuptools import find_packages, setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

classifiers = [
    'Development Status :: 4 - Beta',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.5',
    'Topic :: Software Development :: Debuggers',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(
    name='rowgenerators',
    version="0.9.21",
    description='Generate row data from a variety of file formats',
    long_description=readme,
    packages=find_packages(),
    install_requires=[
        'pyproj',
        'python-dateutil',
        'boto',
        'codegen',
        'decorator',
        'filelock',
        'fs >= 2',
        'geoid',
        'livestats',
        'meta',
        'petl',
        'requests',
        'tabulate',
        'xlrd<2',
        'aniso8601',
        'geopandas',
        'pyyaml',
        'h5py',
        'wrapt'
    ],
    extras_require={
        'geo': ['fiona', 'shapely','pyproj', 'pyproject']
    },
    test_requires=['aniso8601', 'dateutil', 'fiona', 'shapely','pyproj', 'pyproject', 'contexttimer'],
    entry_points={
        'console_scripts': [
            'rowgen=rowgenerators.cli:rowgen',
            'rowgen-generators=rowgenerators.cli:listrowgen',
            'rowgen-urls=rowgenerators.appurl.cli:appurl',
            'rowgen-valuestypes=rowgenerators.valuetype.cli:valuetypes'
        ],

        'appurl.urls': [
            "* = rowgenerators.appurl.url:Url",
            # Web Urls
            "http: = rowgenerators.appurl.web.web:WebUrl",
            "https: = rowgenerators.appurl.web.web:WebUrl",
            "ftp: = rowgenerators.appurl.web.web:FtpUrl",
            "s3: = rowgenerators.appurl.web.s3:S3Url",
            "socrata+ = rowgenerators.appurl.web.socrata:SocrataUrl",
            #
            # Archive Urls
            ".zip = rowgenerators.appurl.archive.zip:ZipUrl",
            #
            # File Urls
            ".csv = rowgenerators.appurl.file.csv:CsvFileUrl",
            ".xlsx = rowgenerators.appurl.file.excel:ExcelFileUrl",
            ".xls = rowgenerators.appurl.file.excel:ExcelFileUrl",
            ".dta = rowgenerators.appurl.file.stata:StataUrl",
            "file: = rowgenerators.appurl.file.file:FileUrl",
            "program+ = rowgenerators.appurl.file.program:ProgramUrl",
            "python: = rowgenerators.appurl.file.python:PythonUrl",
            ".py = rowgenerators.appurl.file.python:PythonUrl",

            "shape+ = rowgenerators.appurl.file.shapefile:ShapefileUrl",
            "/docs.google.com/spreadsheets/ = rowgenerators.appurl.web.google:GoogleSpreadsheetUrl",
            "gs: = rowgenerators.appurl.web.google:GoogleSpreadsheetUrl",
            "gs+ = rowgenerators.appurl.web.google:GoogleSpreadsheetUrl",

            #Sql Alchemy
            "oracle: = rowgenerators.appurl.sql:OracleSql",
            "sql: = rowgenerators.appurl.sql:InlineSqlUrl",
            ".h5 = rowgenerators.appurl.file.hdf5:Hdf5Url",
            ".hdf5 = rowgenerators.appurl.file.hdf5:Hdf5Url",
        ],

        'rowgenerators': [
            "<iterator> = rowgenerators.generator.iterator:IteratorSource",
            "<generator> = rowgenerators.generator.generator:GeneratorSource",
            ".csv = rowgenerators.generator.csv:CsvSource",
            ".tsv = rowgenerators.generator.delimited:TsvSource",
            ".pipe = rowgenerators.generator.delimited:PipeSource",
            ".xlsx = rowgenerators.generator.excel:ExcelSource",
            ".xls =  rowgenerators.generator.excel:ExcelSource",
            ".dta =  rowgenerators.generator.stata:StataSource",
            "program+ = rowgenerators.generator.program:ProgramSource",
            "shape+ = rowgenerators.generator.shapefile:ShapefileSource",
            ".geojson = rowgenerators.generator.shapefile:GeoJsonSource",
            ".shp = rowgenerators.generator.shapefile:ShapefileSource",
            "<PythonUrl> = rowgenerators.generator.python:PythonSource",
            "fixed+ = rowgenerators.generator.fixed:FixedSource",
            "<Sql> = rowgenerators.generator.sql:SqlSource",
            ".h5 = rowgenerators.generator.hdf5:Hdf5Source",
            ".hdf5 = rowgenerators.generator.hdf5:Hdf5Source",
        ],
    },

    author="Eric Busboom",
    author_email='eric@civicknowledge.com',
    url='https://github.com/Metatab/rowgenerator.git',
    license='MIT',
    classifiers=classifiers
)
