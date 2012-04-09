from setuptools import setup
from distutils.extension import Extension

import elasticpy

setup(
    name='elasticpy',
    version=elasticpy.__version__,
    description='Python Wrapper for elasticsearch',
    author='Luke Campbell',
    author_email='luke.s.campbell@gmail.com',
    url='http://lukecampbell.github.com',
    license='Apache 2.0',
    py_modules=['elasticpy'],
    data_files = [('.', ['COPYING', 'README.md'])]
)
