"""
Usage example for `peewee-async`_ with `Tornado`_ web framewotk.

Asynchronous interface for `peewee`_ ORM powered by `asyncio`_:
https://github.com/05bit/peewee-async

.. _peewee-async: https://github.com/05bit/peewee-async
.. _Tornado: http://www.tornadoweb.org

Licensed under The MIT License (MIT)

Copyright (c) 2016, Alexey Kinëv <rudy@05bit.com>

"""
import asyncio
import logging

import peewee

import peewee_async
# Start example [marker for docs]
import tornado.web
# Set up Tornado application on asyncio
from tornado.platform.asyncio import AsyncIOMainLoop

# Set up database and manager
database = peewee_async.PooledPostgresqlDatabase('test')

# Define model
class TestNameModel(peewee.Model):
    name = peewee.CharField()
    class Meta:
        database = database

    def __str__(self):
        return self.name

# Create table, add some instances
TestNameModel.create_table(True)
TestNameModel.get_or_create(id=1, defaults={'name': "TestNameModel id=1"})
TestNameModel.get_or_create(id=2, defaults={'name': "TestNameModel id=2"})
TestNameModel.get_or_create(id=3, defaults={'name': "TestNameModel id=3"})
database.close()

AsyncIOMainLoop().install()
app = tornado.web.Application(debug=True)
app.listen(port=8888)
app.objects = peewee_async.Manager(database)

# Add handlers
class RootHandler(tornado.web.RequestHandler):
    """Accepts GET and POST methods.

    POST: create new instance, `name` argument is required
    GET: get instance by id, `id` argument is required
    """
    async def post(self):
        name = self.get_argument('name')
        obj = await self.application.objects.create(TestNameModel, name=name)
        self.write({
            'id': obj.id,
            'name': obj.name
        })
  
    async def get(self):
        obj_id = self.get_argument('id', None)

        if not obj_id:
            self.write("Please provide the 'id' query argument, i.e. ?id=1")
            return

        try:
            obj = await self.application.objects.get(TestNameModel, id=obj_id)
            self.write({
                'id': obj.id,
                'name': obj.name,
            })
        except TestNameModel.DoesNotExist:
            raise tornado.web.HTTPError(404, "Object not found!")

class CreateHandler(tornado.web.RequestHandler):
    async def get(self):
        loop = asyncio.get_event_loop()
        task1 = asyncio.Task.current_task() # Just to demonstrate it's None
        task2 = loop.create_task(self.get_or_create())
        obj = await task2
        self.write({
            'task1': task1 and id(task1),
            'task2': task2 and id(task2),
            'obj': str(obj),
            'text': "'task1' should be null, "
                    "'task2' should be not null, "
                    "'obj' should be newly created object",
        })

    async def get_or_create(self):
        obj_id = self.get_argument('id', None)
        async with self.application.objects.atomic():
            obj, created = await self.application.objects.get_or_create(
                TestNameModel, id=obj_id,
                defaults={'name': "TestNameModel id=%s" % obj_id})
            return obj

app.add_handlers('', [
    (r"/", RootHandler),
    (r"/create", CreateHandler),
])

# Setup verbose logging
log = logging.getLogger('')
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)

# Run loop
print("""Run application server http://127.0.0.1:8888

    Try GET urls:
    http://127.0.0.1:8888?id=1
    http://127.0.0.1:8888/create?id=100

    Try POST with name=<some text> data:
    http://127.0.0.1:8888

^C to stop server""")
loop = asyncio.get_event_loop()
try:
    loop.run_forever()
except KeyboardInterrupt:
    print(" server stopped")
