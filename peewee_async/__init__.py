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

from peewee_async.aio_model import AioModel, aio_prefetch
from peewee_async.connection import connection_context
from peewee_async.databases import (
    PooledMySQLDatabase,
    PooledPostgresqlDatabase,
    PooledPostgresqlExtDatabase,
    PsycopgDatabase,
)
from peewee_async.pool import MysqlPoolBackend, PostgresqlPoolBackend
from peewee_async.transactions import Transaction

__version__ = version("peewee-async")


__all__ = [
    "PooledPostgresqlDatabase",
    "PooledPostgresqlExtDatabase",
    "PooledMySQLDatabase",
    "Transaction",
    "AioModel",
    "aio_prefetch",
    "connection_context",
    "PostgresqlPoolBackend",
    "MysqlPoolBackend",
]

register_database(PooledPostgresqlDatabase, "postgres+pool+async", "postgresql+pool+async")
register_database(PooledPostgresqlExtDatabase, "postgresext+pool+async", "postgresqlext+pool+async")
register_database(PsycopgDatabase, "psycopg+pool+async", "psycopg+pool+async")
register_database(PooledMySQLDatabase, "mysql+pool+async")
