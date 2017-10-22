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
import logging

from src.transactions import transaction
from src.transactions import atomic
from src.transactions import savepoint

from src.postgres import PooledPostgresqlDatabase
from src.postgres import PostgresqlDatabase
from src.postgres import AsyncPostgresqlMixin
from src.mysql import MySQLDatabase
from src.mysql import PooledMySQLDatabase
from src.async_db import UnwantedSyncQueryError
from src.async_db import sync_unwanted

from src.queries import get_object
from src.queries import delete_object
from src.queries import create_object
from src.queries import update_object
from src.queries import scalar
from src.queries import execute
from src.queries import count

from src.manager import Manager

logger = logging.getLogger('peewee.async')
logger.addHandler(logging.NullHandler())

__version__ = '0.5.7'

__all__ = [
    # High level API

    'Manager',
    'PostgresqlDatabase',
    'PooledPostgresqlDatabase',
    'AsyncPostgresqlMixin',
    'MySQLDatabase',
    'PooledMySQLDatabase',

    # Low level API

    'execute',
    'count',
    'scalar',
    'atomic',
    'transaction',
    'savepoint',

    # Deprecated

    'get_object',
    'create_object',
    'delete_object',
    'update_object',
    'sync_unwanted',
    'UnwantedSyncQueryError',
]
