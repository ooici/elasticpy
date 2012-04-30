from setuptools import setup
from distutils.extension import Extension



setup(
    name='elasticpy',
    version='0.7',
    description='Python Wrapper for elasticsearch',
    author='Luke Campbell',
    author_email='LCampbell@ASAScience.com',
    url='http://github.com/ooici/elasticpy/',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database :: Front-Ends',
        'Topic :: System :: Software Distribution',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities'
        ],
    license='Apache 2.0',
    keywords='elasticsearch search wrapper',
    py_modules=['elasticpy'],
    install_requires = [
        'Requests==0.11.2'
    ],

)
