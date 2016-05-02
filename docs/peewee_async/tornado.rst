Using async peewee with Tornado
===============================

`Tornado`_ is a mature and powerful asynchronous web framework. It provides its own event loop, but there's an option to run Tornado on asyncio event loop. And that's exactly what we need!

.. _Tornado: http://www.tornadoweb.org

The complete working example is provided below. And here are some general notes:

1. **Be aware of current asyncio event loop!**

  In the provided example we use the default event loop everywhere, and that's OK. But if you see your application got silently stuck, that's most probably that some task is started on different loop and will never complete as long as that loop is not running.

2. Tornado request handlers **does not** start asyncio tasks by default.

  The ``CreateHandler`` demostrates that, ``current_task()`` returns ``None`` until taks is run explicitly.

3. Transactions **must** run within task context.

  All transaction operations have to be done within task. So if you need to run a transaction from Tornado handler, you have to wrap your call into task with ``create_task()`` or ``ensure_future()``.

  **Also note:** if you spawn an extra task during transaction, it will run outside of transaction.

.. code-block:: python

    import tornado.web
    import logging
    import peewee
    import asyncio
    import peewee_async

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

    # Set up Tornado application on asyncio
    from tornado.platform.asyncio import AsyncIOMainLoop
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
            task1 = asyncio.Task.current_task()
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
