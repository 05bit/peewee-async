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

setup(
    name="peewee-async",
    version=__version__,
    author="Alexey Kinev",
    author_email='rudy@05bit.com',
    url='https://github.com/05bit/peewee-async',
    description=__doc__,
    # long_description=__doc__,
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
