aiopeewee = asyncio + peewee
============================

Asynchronous interface for **[peewee](https://github.com/coleifer/peewee)**
orm powered by **[asyncio](https://docs.python.org/3/library/asyncio.html)**.

Documentation
-------------

http://python-aiopeewee.readthedocs.org

Install
-------

Works on Python 3.3+ and PostgreSQL database.

Install with `pip`:

```
pip install aiopeewee
```

Quickstart
----------

Create test PostgreSQL database, i.e. 'test' for running this snippet:

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
database.close()

@asyncio.coroutine
def my_handler():
    TestModel.create(text="Yo, I can do it sync!")
    yield from aiopeewee.create_object(TestModel, text="Not bad. Watch this, I'm async!")
    all_objects = yield from aiopeewee.execute(TestModel.select())
    for obj in all_objects:
        print(obj.text)

loop.run_until_complete(database.connect_async(loop=loop))
loop.run_until_complete(my_handler())
```

Discuss
-------

You are welcome to add discussion topics or bug reports to tracker on GitHub: https://github.com/05bit/python-aiopeewee/issues

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
