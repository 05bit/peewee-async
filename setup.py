"""
Asynchronous interface for peewee ORM powered by asyncio.
"""
import os
from setuptools import setup

__version__ = ''

with open('peewee_async.py') as file:
    for line in file:
        if line.startswith('__version__'):
            __version__ = line.split('=')[1].strip().strip("'").strip('"')
            break

long_description = \
"""Current state: **alpha**, yet API seems fine and mostly stable.

In current version (0.5.x) new-high level API is introduced while older low-level API partially marked as deprecated.

* Works on Python 3.4+
* Has support for PostgreSQL via `aiopg`
* Has support for MySQL via `aiomysql`
* Single point for high-level async API
* Drop-in replacement for sync code, sync will remain sync
* Basic operations are supported
* Transactions support is present, yet not heavily tested

The source code is hosted on `GitHub`_.

.. _GitHub: https://github.com/05bit/peewee-async
"""

setup(
    name="peewee-async",
    version=__version__,
    author="Alexey Kinev",
    author_email='rudy@05bit.com',
    url='https://github.com/05bit/peewee-async',
    description=__doc__,
    long_description=long_description,
    license='MIT',
    zip_safe=False,
    install_requires=(
        'peewee >= 2.8.0',
    ),
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
