"""
Asynchronous interface for peewee ORM powered by asyncio.
"""
from setuptools import setup
import peewee_async

__version__ = peewee_async.__version__

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
        'peewee>=2.6.1',
        'aiopg>=0.7.0',
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
)
