"""
peewee-async
============

Asynchronous interface for `peewee`_ ORM powered by `asyncio`_:
https://github.com/05bit/peewee-async

.. _peewee: https://github.com/coleifer/peewee
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Licensed under The MIT License (MIT)

Copyright (c) 2014, Alexey Kinëv <rudy@05bit.com>

"""
import warnings
from peewee_async import PooledPostgresqlExtDatabase, PostgresqlExtDatabase

warnings.warn(
    "import from `peewee_asyncext is deprecated the module will be removed",
    DeprecationWarning
)
