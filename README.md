peewee-async
============

Asynchronous interface for **[peewee](https://github.com/coleifer/peewee)**
ORM powered by **[asyncio](https://docs.python.org/3/library/asyncio.html)**.

![CI workflow](https://github.com/05bit/peewee-async/actions/workflows/ci.yml/badge.svg) [![PyPi Version](https://img.shields.io/pypi/v/peewee-async.svg)](https://pypi.python.org/pypi/peewee-async)
 [![Documentation Status](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat)](http://peewee-async.readthedocs.io/en/latest/?badge=latest)

Important notes
---------------

- Since version `0.6.0a` only **peewee 3.5+** is supported
- If you still need Python 3.5 support use older versions, i.e. `pip install peewee-async==0.7.2`


Overview
--------

* Requires Python 3.6+
* Has support for PostgreSQL via [aiopg](https://github.com/aio-libs/aiopg)
* Has support for MySQL via [aiomysql](https://github.com/aio-libs/aiomysql)
* Single point for high-level async API
* Drop-in replacement for sync code, sync will remain sync
* Basic operations are supported
* Transactions support is present, yet not heavily tested

The complete documentation:  
http://peewee-async.readthedocs.io

Install
-------

Install with `pip` for PostgreSQL:

```
pip install --pre peewee-async; pip install aiopg
```

or for MySQL:

```
pip install --pre peewee-async; pip install aiomysql
```

Quickstart
----------

Create 'test' PostgreSQL database for running this snippet:

    createdb -E utf-8 test

The code below is using new Python 3.6 `async` / `await` syntax
```python
import asyncio
import peewee
import peewee_async

# Nothing special, just define model and database:

database = peewee_async.PostgresqlDatabase(
    database='db_name',
    user='user',
    host='127.0.0.1',
    port='5432',
    password='password'
)

class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database

# Look, sync code is working!

TestModel.create_table(True)
TestModel.create(text="Yo, I can do it sync!")
database.close()

# Create async models manager:

objects = peewee_async.Manager(database)

# No need for sync anymore!

database.set_allow_sync(False)

async def handler():
    await objects.create(TestModel, text="Not bad. Watch this, I'm async!")
    all_objects = await objects.execute(TestModel.select())
    for obj in all_objects:
        print(obj.text)

loop = asyncio.get_event_loop()
loop.run_until_complete(handler())
loop.close()

# Clean up, can do it sync again:
with objects.allow_sync():
    TestModel.drop_table(True)

# Expected output:
# Yo, I can do it sync!
# Not bad. Watch this, I'm async!
```

Documentation
-------------

http://peewee-async.readthedocs.io

Developing
-------------
Install dependencies:
```pip install -e .[develop]```

Run tests: ``` pytest tests -v -s```

Discuss
-------

You are welcome to add discussion topics or bug reports to tracker on GitHub: https://github.com/05bit/peewee-async/issues

License
-------

Copyright (c) 2014, Alexey Kinev <rudy@05bit.com>

Licensed under The MIT License (MIT),
see LICENSE file for more details.
