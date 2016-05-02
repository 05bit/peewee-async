Using async peewee with Tornado
===============================

`Tornado`_ is a mature and powerful asynchronous web framework. It provides its own event loop, but there's an option to run Tornado on asyncio event loop. And that's exactly what we need!

.. _Tornado: http://www.tornadoweb.org

The complete working example is provided below. And here are some general notes:

1. **Be aware of current asyncio event loop!**

  In the provided example we use the default event loop everywhere, and that's OK. But if you see your application got silently stuck, that's most probably that some task is started on the different loop and will never complete as long as that loop is not running.

2. Tornado request handlers **does not** start asyncio tasks by default.

  The ``CreateHandler`` demostrates that, ``current_task()`` returns ``None`` until task is run explicitly.

3. Transactions **must** run within task context.

  All transaction operations have to be done within task. So if you need to run a transaction from Tornado handler, you have to wrap your call into task with ``create_task()`` or ``ensure_future()``.

  **Also note:** if you spawn an extra task during a transaction, it will run outside of that transaction.

.. literalinclude:: ../../examples/tornado_sample.py
  :start-after: # Start example
