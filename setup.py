from setuptools import setup
from distutils.extension import Extension

import elasticpy

setup(
    name='elasticpy',
    version=elasticpy.__version__,
    description='Python Wrapper for elasticsearch',
    author='Luke Campbell',
    author_email='LCampbell@ASAScience.com',
    url='http://github.com/ooici/elasticpy/',
    license='Apache 2.0',
    py_modules=['elasticpy'],
    data_files = [('.', ['COPYING', 'README.md'])]
)
