import abc
import asyncio

from .utils import aiopg, aiomysql


class PoolBackend(metaclass=abc.ABCMeta):
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database=None, **kwargs):
        self.pool = None
        self.database = database
        self.connect_params = kwargs
        self._connection_lock = asyncio.Lock()

    @property
    def is_connected(self):
        return self.pool is not None and self.pool.closed is False

    def has_acquired_connections(self):
        return self.pool is not None and len(self.pool._used) > 0

    async def connect(self):
        async with self._connection_lock:
            if self.is_connected is False:
                await self.create()

    async def acquire(self):
        """Acquire connection from pool.
        """
        if self.pool is None:
            await self.connect()
        return await self.pool.acquire()

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    @abc.abstractmethod
    async def create(self):
        """Create connection pool asynchronously.
        """
        raise NotImplementedError

    async def terminate(self):
        """Terminate all pool connections.
        """
        if self.pool is not None:
            self.pool.terminate()
            await self.pool.wait_closed()


class PostgresqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self):
        """Create connection pool asynchronously.
        """
        if "connect_timeout" in self.connect_params:
            self.connect_params['timeout'] = self.connect_params.pop("connect_timeout")
        self.pool = await aiopg.create_pool(
            database=self.database,
            **self.connect_params
        )


class MysqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self):
        """Create connection pool asynchronously.
        """
        self.pool = await aiomysql.create_pool(
            db=self.database, **self.connect_params
        )
