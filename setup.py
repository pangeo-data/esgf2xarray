#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import sys
from setuptools import setup, find_packages
import versioneer
from os.path import exists

if exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = ''

install_requires = ["xarray", "dask", "zarr", "tqdm", "requests"]

test_requirements = ['pytest']

setup(
    maintainer='Ryan Abernathey',
    maintainer_email='',
    description='Utilities for loading esgf archives as xarray datasets',
    install_requires=install_requires,
    license='Apache License 2.0',
    long_description=long_description,
    name='esgf2xarray',
    packages=find_packages(),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/pangeo-data/esgf2xarray',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    zip_safe=False,
)