"""
peewee-async
============

Asynchronous interface for `peewee`_ ORM powered by `asyncio`_:
https://github.com/05bit/peewee-async

.. _peewee: https://github.com/coleifer/peewee
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Copyright 2014 Alexey Kinev, 05Bit http://05bit.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
from peewee_async import AsyncPostgresqlMixin, PooledAsyncConnection
import playhouse.postgres_ext as ext


class PostgresqlExtDatabase(AsyncPostgresqlMixin, ext.PostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **single async connection** interface.

    See also:
    https://peewee.readthedocs.org/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """
    def __init__(self, database, threadlocals=True, autocommit=True,
                 fields=None, ops=None, autorollback=True, **kwargs):
        super().__init__(database, threadlocals=True, autocommit=autocommit,
                         fields=fields, ops=ops, autorollback=autorollback,
                         **kwargs)

        async_kwargs = self.connect_kwargs.copy()
        async_kwargs.update({
            'enable_json': True,
            'enable_hstore': self.register_hstore,
        })
        self.init_async(**async_kwargs)


class PooledPostgresqlExtDatabase(AsyncPostgresqlMixin, ext.PostgresqlExtDatabase):
    """PosgreSQL database extended driver providing **single drop-in sync**
    connection and **async connections pool** interface.

    :param max_connections: connections pool size

    See also:
    https://peewee.readthedocs.org/en/latest/peewee/playhouse.html#PostgresqlExtDatabase
    """
    def __init__(self, database, threadlocals=True, autocommit=True,
                 fields=None, ops=None, autorollback=True, max_connections=20,
                 **kwargs):
        super().__init__(database, threadlocals=True, autocommit=autocommit,
                         fields=fields, ops=ops, autorollback=autorollback,
                         **kwargs)

        async_kwargs = self.connect_kwargs.copy()
        async_kwargs.update({
            'enable_json': True,
            'enable_hstore': self.register_hstore,
        })
        self.init_async(conn_cls=PooledAsyncConnection, minsize=1,
                        maxsize=max_connections, **async_kwargs)
