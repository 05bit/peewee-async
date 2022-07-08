"""
Asynchronous interface for peewee ORM powered by asyncio.
"""
from setuptools import setup

VERSION = ''

with open('peewee_async.py') as module_fp:
    for line in module_fp:
        if line.startswith('__version__'):
            VERSION = line.split('=')[1].strip().strip("'").strip('"')
            break

with open('README.md', 'r') as readme_fp:
    LONG_DESCRIPTION = readme_fp.read()

# Usage hints:
#
#     python setup.py develop
#     pip install -e .[develop]
#     python setup.py sdist bdist_wheel
#     twine check dist/* 
#

setup(
    name="peewee-async",
    version=VERSION,
    author="Alexey Kinev",
    author_email='rudy@05bit.com',
    url='https://github.com/05bit/peewee-async',
    description=__doc__.strip(),
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    license='MIT',
    zip_safe=False,
    install_requires=(
        'peewee>=3.5.0,<4.0',
    ),
    extras_require={
        'develop': [
            'pylint',
            'wheel',
            'twine',
            'aiomysql',
            'aiopg',
            'pytest',
            'pytest-asyncio'
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
)
