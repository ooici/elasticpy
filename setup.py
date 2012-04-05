from setuptools import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import elasticpy

setup(
    name='elasticpy',
    version='0.1',
    description='Python Wrapper for elasticsearch',
    author='Luke Campbell',
    author_email='luke.s.campbell@gmail.com',
    url='http://lukecampbell.github.com',
    license='Apache 2.0',
    py_modules=['elasticpy'],
    data_files = [('.', ['COPYING', 'README.md'])]
)
