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
from peewee_async import AsyncPostgresqlMixin
import playhouse.postgres_ext as ext


class PostgresqlExtDatabase(AsyncPostgresqlMixin, ext.PostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **single async connection** interface.

    See also:
    https://peewee.readthedocs.org/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """
    def init(self, database, **kwargs):
        super().init(database, **kwargs)
        self.init_async(enable_json=True,
                        enable_hstore=self.register_hstore)


class PooledPostgresqlExtDatabase(AsyncPostgresqlMixin, ext.PostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    See also:
    https://peewee.readthedocs.org/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """
    def init(self, database, **kwargs):
        super().init(database, **kwargs)
        self.init_async(enable_json=True,
                        enable_hstore=self.register_hstore)
        self.min_connections = self.connect_kwargs.pop('min_connections', 1)
        self.max_connections = self.connect_kwargs.pop('max_connections', 20)

    @property
    def connect_kwargs_async(self):
        """Connection parameters for `aiopg.Pool`
        """
        kwargs = super().connect_kwargs_async
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
        })
        return kwargs
