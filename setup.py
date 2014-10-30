"""
Asynchronous interface for peewee ORM powered by asyncio.
"""
from setuptools import setup

__version__ = '0.1'

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
        'peewee>=2.4.1',
        'aiopg>=0.4.1',
    ),
    # packages=find_packages(),
    py_modules=['peewee_async'],
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
