aiopeewee = asyncio + peewee
============================

Asynchronous interface for **[peewee](https://github.com/coleifer/peewee)**
orm powered by **[asyncio](https://docs.python.org/3/library/asyncio.html)**.

Current state: **proof of concept**.

Module provides database backends and wrappers for performing asynchronous
queries.

Install
-------

1. Python 3.3+ is required

2. Install latest [aiopg](https://github.com/aio-libs/aiopg/) from GitHub
    ```
    pip install -e git+https://github.com/aio-libs/aiopg.git#egg=aiopg
    ```

3. Install development version
    ```
    git clone https://github.com/05bit/python-aiopeewee.git
    cd python-aiopeewee
    python setup.py develop
    ```

Databases
---------

    class PostgresqlDatabase(peewee.PostgresqlDatabase)
    class PooledPostgresqlDatabase(peewee.PostgresqlDatabase)

It provides a drop-in sync interface and extra async interface, so it's
possible to migrate codebase from sync to async without breaking everything
at once.

The tradeoff is that **two connections may be opened at one time** -- one for sync and
another for async mode. And yet we don't prevent queries from (occasional) syncronous
blocking evaluation! We may probably need async only interface later. Maybe not.

Some thoughts on that:

* Initial setup e.g. tables creation is generally easier and it's ok to run **synchronously**
  before starting event loop.
* Peewee interface should remain peewee interface and it's originally syncronous, we may **extend**
  the interface but not break good old one.
* We may prefetch related objects explicitly and have a kind of "warning mode"
  to get notified (logged) on unwanted blocking queries execution.

Wrappers
--------

    create(cls, model, **query)
    delete_instance(obj, recursive=False, delete_nullable=False)
    select(query)
    save(obj, force_insert=False, only=None)
    update(query)
    delete(query)

All wrappers are asyncio coroutines.

Not implemented::

* transactions, see http://aiopg.readthedocs.org/en/0.3/core.html#transactions
* aggregated queries produced with aggregate_rows()
* count(), scalar() queries

Basic example
-------------

```python
import asyncio
import peewee
import aiopeewee

database = aiopeewee.PostgresqlDatabase('test')
loop = asyncio.get_event_loop()

class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database

# Create table synchronously!
TestModel.create_table(True)
# This is optional: close sync connection
# database.close()

@asyncio.coroutine
def my_handler():
    # Open async connection in place to simplify example
    yield from database.connect(loop=loop)
    all_objects = yield from aiopeewee.select(TestModel.select())
    database.close()

loop.run_until_complete(my_handler())
```

Wrapping is possible because peewee queries are lazy objects. So you may
construct them in generic way, you should just avoid avaluation. At this time
module does not provide asynchronous reading of related objects.

Discuss
-------

Project state is "poof of concept proto", so feel free to add discussion
topics to issue tracker: https://github.com/05bit/python-aiopeewee/issues

License
-------

Copyright 2014 Alexey Kinev, 05Bit http://05bit.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
