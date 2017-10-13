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
    version="0.7.4",
    description='Generate row data from a variety of file formats',
    long_description=readme,
    packages=find_packages(),
    install_requires=[
        'appurl >= 0.1.5',
        'fs >= 2',
        'boto',
        'requests',
        'petl',
        'livestats',
        'filelock',
        'tabulate',
    ],
    extras_require={
        'geo': ['fiona', 'shapely','pyproj', 'pyproject']
    },
    entry_points={
        'console_scripts': [
            'rowgen=rowgenerators.cli:rowgen',
        ],

        'rowgenerators': [
            "<iterator> = rowgenerators.generator.iterator:IteratorSource",
            "<generator> = rowgenerators.generator.generator:GeneratorSource",
            ".csv = rowgenerators.generator.csv:CsvSource",
            ".tsv = rowgenerators.generator.tsv:TsvSource",
            ".xlsx = rowgenerators.generator.excel:ExcelSource",
            ".xls =  rowgenerators.generator.excel:ExcelSource",
            "program+ = rowgenerators.generator.program:ProgramSource",
            "jupyter+ = rowgenerators.generator.jupyter:NotebookSource",
            "shape+ = rowgenerators.generator.shapefile:ShapefileSource",
            "python: = rowgenerators.generator.python:PythonSource",
            "fixed+ = rowgenerators.generator.fixed:FixedSource",
        ],
        'appurl.urls': [
            "shape+ = rowgenerators.appurl.shapefile:ShapefileUrl",

        ]


    },
    author="Eric Busboom",
    author_email='eric@civicknowledge.com',
    url='https://github.com/CivicKnowledge/rowgenerator.git',
    license='MIT',
    classifiers=classifiers
)
