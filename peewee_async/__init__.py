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
from importlib.metadata import version

from playhouse.db_url import register_database
from .aio_model import aio_prefetch, AioModel
from .connection import connection_context
from .databases import (
    PooledPostgresqlDatabase,
    PooledPostgresqlExtDatabase,
    PooledMySQLDatabase,
)
from .pool import PostgresqlPoolBackend, MysqlPoolBackend
from .transactions import Transaction

__version__ = version('peewee-async')


__all__ = [
    'PooledPostgresqlDatabase',
    'PooledPostgresqlExtDatabase',
    'PooledMySQLDatabase',
    'Transaction',
    'AioModel',
    'aio_prefetch',
    'connection_context',
    'PostgresqlPoolBackend',
    'MysqlPoolBackend',
]

register_database(PooledPostgresqlDatabase, 'postgres+pool+async', 'postgresql+pool+async')
register_database(PooledPostgresqlExtDatabase, 'postgresext+pool+async', 'postgresqlext+pool+async')
register_database(PooledMySQLDatabase, 'mysql+pool+async')
