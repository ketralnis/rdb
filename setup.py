#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()
from setuptools import find_packages#, setup

from distutils.core import setup

try:
    import tornado
except ImportError:
    easy_install(['http://www.tornadoweb.org/static/tornado-0.2.tar.gz'])

setup(name='rdb',
      version='0.1',
      description='rdb key value database',
      author='ketralnis',
      author_email='ketralnis@reddit.com',
      install_requires=['python-memcached',
                        'bsddb3',
                        'urllib3'],
      packages=[],
     )
