"""
Asynchronous interface for peewee ORM powered by asyncio.
"""
from setuptools import setup

__version__ = ''

with open('peewee_async.py') as module_fp:
    for line in module_fp:
        if line.startswith('__version__'):
            __version__ = line.split('=')[1].strip().strip("'").strip('"')
            break

LONG_DESCRIPTION = \
"""Current state: **alpha**, yet API seems fine and mostly stable.

Since version 0.6.0 peewee-async is compatible with peewee 3.5+ and
support of Python 3.4 is dropped.

* Works on Python 3.5+
* Required peewee 3.5+
* Has support for PostgreSQL via `aiopg`
* Has support for MySQL via `aiomysql`
* Single point for high-level async API
* Drop-in replacement for sync code, sync will remain sync
* Basic operations are supported
* Transactions support is present, yet not heavily tested

The source code is hosted on `GitHub`_.

.. _GitHub: https://github.com/05bit/peewee-async
"""

# Usage hints:
#
#     python setup.py develop
#     pip install -e .[develop]
#     python setup.py sdist bdist_wheel upload
#

setup(
    name="peewee-async",
    version=__version__,
    author="Alexey Kinev",
    author_email='rudy@05bit.com',
    url='https://github.com/05bit/peewee-async',
    description=__doc__.strip(),
    long_description=LONG_DESCRIPTION,
    license='MIT',
    zip_safe=False,
    install_requires=(
        'peewee>=3.5.0,<4.0',
    ),
    extras_require={
        'develop': [
            'pylint',
            'wheel',
            'aiomysql',
            'aiopg',
            'psycopg2'
        ],
        'aiopg': ['aiopg>=0.14.0'],
        'aiomysql': ['aiomysql>=0.0.19'],
    },
    py_modules=[
        'peewee_async',
        'peewee_asyncext'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ],
    test_suite='tests',
    test_loader='unittest:TestLoader',
)
