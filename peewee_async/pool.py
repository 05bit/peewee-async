import abc
import asyncio
from typing import Any, Generic

from .utils import aiopg, aiomysql, T_Connection


class PoolBackend(Generic[T_Connection], metaclass=abc.ABCMeta):
    """Asynchronous database connection pool.
    """
    def __init__(self, *, database: str, **kwargs: Any) -> None:
        self.pool = None
        self.database = database
        self.connect_params = kwargs
        self._connection_lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        if self.pool is not None:
            return self.pool.closed is False
        return False

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return len(self.pool._used) > 0
        return False

    async def connect(self) -> None:
        async with self._connection_lock:
            if self.is_connected is False:
                await self.create()

    async def acquire(self) -> T_Connection:
        """Acquire connection from pool.
        """
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return await self.pool.acquire()

    def release(self, conn: T_Connection) -> None:
        """Release connection to pool.
        """
        assert self.pool is not None, "Pool is not connected"
        self.pool.release(conn)

    @abc.abstractmethod
    async def create(self) -> None:
        """Create connection pool asynchronously.
        """
        raise NotImplementedError

    async def terminate(self) -> None:
        """Terminate all pool connections.
        """
        if self.pool is not None:
            self.pool.terminate()
            await self.pool.wait_closed()


class PostgresqlPoolBackend(PoolBackend[aiopg.Connection]):
    """Asynchronous database connection pool.
    """

    async def create(self) -> None:
        """Create connection pool asynchronously.
        """
        if "connect_timeout" in self.connect_params:
            self.connect_params['timeout'] = self.connect_params.pop("connect_timeout")
        self.pool = await aiopg.create_pool(
            database=self.database,
            **self.connect_params
        )


class MysqlPoolBackend(PoolBackend[aiomysql.Connection]):
    """Asynchronous database connection pool.
    """

    async def create(self) -> None:
        """Create connection pool asynchronously.
        """
        self.pool = await aiomysql.create_pool(
            db=self.database, **self.connect_params
        )
