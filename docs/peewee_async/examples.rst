Usage examples
==============

Using with Tornado
------------------

.. code-block:: python

    import tornado.gen
    import tornado.web
    from tornado.platform.asyncio import AsyncIOMainLoop
    import peewee
    import asyncio
    import peewee_async

    # Set up asincio loop for Tornado
    AsyncIOMainLoop().install()
    loop = asyncio.get_event_loop()

    # Create application
    application = tornado.web.Application(debug=True)
    application.listen(port=8888)

    # Set up database connection
    database = peewee_async.PooledPostgresqlDatabase('test')
    application.db = database

    # Define model, handler and run application:


    class TestNameModel(peewee.Model):
        name = peewee.CharField()
        class Meta:
            database = database


    class TestHandler(tornado.web.RequestHandler):
      @tornado.gen.coroutine
      def post(self):
        name = self.get_argument('name')
        obj = yield from peewee_async.create_object(TestNameModel, name=name)
        self.write({'id': obj.id, 'name': obj.name})

      @tornado.gen.coroutine
      def get(self):
        obj_id = self.get_argument('id')
        try:
            obj = yield from peewee_async.get_object(TestNameModel, TestNameModel.id == obj_id)
            self.write({'id': obj.id, 'name': obj.name})
        except TestNameModel.DoesNotExist:
            raise tornado.web.HTTPError(404, "Object not found!")


    application.add_handlers('', [
       (r"/test", TestHandler)
    ])


    # Create database table
    TestNameModel.create_table(True)
    database.close()

    # Set up async connection and run application server
    loop.run_until_complete(application.db.connect_async())
    loop.run_forever()


Using both sync and async calls
-------------------------------

.. code-block:: python

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
        obj1 = TestModel.create(text="Yo, I can do it sync!")
        obj2 = yield from peewee_async.create_object(TestModel, text="Not bad. Watch this, I'm async!")

        all_objects = yield from peewee_async.execute(TestModel.select())
        for obj in all_objects:
            print(obj.text)

        obj1.delete_instance()
        yield from peewee_async.delete_object(obj2)

    loop.run_until_complete(database.connect_async(loop=loop))
    loop.run_until_complete(my_handler())


Using transactions
------------------

.. code-block:: python

    import asyncio
    import peewee
    import peewee_async

    # ... some init code ...

    async def test():
        obj = await create_object(TestModel, text='FOO')
        obj_id = obj.id

        try:
            async with database.atomic_async():
                obj.text = 'BAR'
                await update_object(obj)
                raise Exception('Fake error')
        except:
            res = await get_object(TestModel, TestModel.id == obj_id)

        print(res.text) # Should print 'FOO', not 'BAR'

    loop.run_until_complete(test())
