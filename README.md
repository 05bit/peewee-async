peewee-async
============

Asynchronous interface for **[peewee](https://github.com/coleifer/peewee)**
ORM powered by **[asyncio](https://docs.python.org/3/library/asyncio.html)**.

[![Build Status](https://travis-ci.org/05bit/peewee-async.svg)](https://travis-ci.org/05bit/peewee-async) [![Documentation Status](https://readthedocs.org/projects/peewee-async/badge/?version=latest)](http://peewee-async.readthedocs.org/en/latest/?badge=latest)
    

Documentation
-------------

http://peewee-async.readthedocs.org

Install
-------

Works on Python 3.3+ and PostgreSQL database.

Install with `pip`:

```
pip install peewee-async
```

Quickstart
----------

Create test PostgreSQL database, i.e. 'test' for running this snippet:

```python
import asyncio
import peewee
import peewee_async

database = peewee_async.PostgresqlDatabase('test')
loop = asyncio.get_event_loop()

class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = database

# Create table synchronously!
TestModel.create_table(True)
# This is optional: close sync connection
database.close()

@asyncio.coroutine
def my_handler():
    TestModel.create(text="Yo, I can do it sync!")
    yield from peewee_async.create_object(TestModel, text="Not bad. Watch this, I'm async!")
    all_objects = yield from peewee_async.execute(TestModel.select())
    for obj in all_objects:
        print(obj.text)

loop.run_until_complete(database.connect_async(loop=loop))
loop.run_until_complete(my_handler())
```

Discuss
-------

You are welcome to add discussion topics or bug reports to tracker on GitHub: https://github.com/05bit/peewee-async/issues

License
-------

Copyright (c) 2014, Alexey Kinev <rudy@05bit.com>

Licensed under The MIT License (MIT),
see LICENSE file for more details.
