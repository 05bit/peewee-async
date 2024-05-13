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

from playhouse import postgres_ext as ext
from playhouse.db_url import register_database

from peewee_async import AioPostgresqlMixin


class PostgresqlExtDatabase(AioPostgresqlMixin, ext.PostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **single async connection** interface.

    JSON fields support is always enabled, HStore supports is enabled by
    default, but can be disabled with ``register_hstore=False`` argument.

    Example::

        database = PostgresqlExtDatabase('test', register_hstore=False)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """

    def init(self, database, **kwargs):
        self.min_connections = 1
        self.max_connections = 1
        super().init(database, **kwargs)
        self.init_async(
            enable_json=True,
            enable_hstore=self._register_hstore
        )


register_database(PostgresqlExtDatabase, 'postgresext+async', 'postgresqlext+async')


class PooledPostgresqlExtDatabase(
    AioPostgresqlMixin,
    ext.PostgresqlExtDatabase
):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    JSON fields support is always enabled, HStore supports is enabled by
    default, but can be disabled with ``register_hstore=False`` argument.

    :param max_connections: connections pool size

    Example::

        database = PooledPostgresqlExtDatabase('test', register_hstore=False,
                                               max_connections=20)

    See also:
    https://peewee.readthedocs.io/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """

    def init(self, database, **kwargs):
        self.min_connections = kwargs.pop('min_connections', 1)
        self.max_connections = kwargs.pop('max_connections', 20)
        connection_timeout = kwargs.pop('connection_timeout', None)
        if connection_timeout is not None:
            warnings.warn(
                "`connection_timeout` is deprecated, use `connect_timeout` instead.",
                DeprecationWarning
            )
            kwargs['connect_timeout'] = connection_timeout
        super().init(database, **kwargs)
        self.init_async(
            enable_json=True,
            enable_hstore=self._register_hstore
        )


register_database(PooledPostgresqlExtDatabase, 'postgresext+pool+async', 'postgresqlext+pool+async')
