import abc
import asyncio
from typing import Any, Optional, cast

from .utils import aiopg, aiomysql, PoolProtocol, ConnectionProtocol, format_dsn, psycopg, psycopg_pool


class PoolBackend(metaclass=abc.ABCMeta):
    """Asynchronous database connection pool.
    """

    def __init__(self, *, database: str, **kwargs: Any) -> None:
        self.pool: Optional[PoolProtocol] = None
        self.database = database
        self.connect_params = kwargs
        self._connection_lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        if self.pool is not None:
            return self.pool.closed is False
        return False

    @property
    def min_size(self) -> int:
        return self.pool.minsize

    @property
    def max_size(self) -> int:
        return self.pool.maxsize

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return len(self.pool._used) > 0
        return False

    async def connect(self) -> None:
        async with self._connection_lock:
            if self.is_connected is False:
                await self.create()

    async def acquire(self) -> ConnectionProtocol:
        """Acquire connection from pool.
        """
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return await self.pool.acquire()

    async def release(self, conn: ConnectionProtocol) -> None:
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


class PostgresqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self) -> None:
        """Create connection pool asynchronously.
        """
        if "connect_timeout" in self.connect_params:
            self.connect_params['timeout'] = self.connect_params.pop("connect_timeout")
        self.pool = cast(
            PoolProtocol,
            await aiopg.create_pool(
                database=self.database,
                **self.connect_params
            )
        )


class Psycopg3PoolBackend(PoolBackend):
    """Asynchronous database connection pool based on psycopg + psycopg_pool libraries.
    """

    async def create(self) -> None:
        """Create connection pool asynchronously.
        """

        pool = psycopg_pool.AsyncConnectionPool(
            format_dsn(
                'postgresql',
                host=self.connect_params['host'],
                port=self.connect_params['port'],
                user=self.connect_params['user'],
                password=self.connect_params['password'],
                path=self.database,
            ),
            min_size=self.connect_params.get('minsize', 1),
            max_size=self.connect_params.get('maxsize', 20),
            max_lifetime=self.connect_params.get('pool_recycle', 60 * 60.0),
            open=False,
            kwargs={
                'cursor_factory': psycopg.AsyncClientCursor,
                'autocommit': True,
            }
        )

        await pool.open()
        self.pool = pool

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return self.pool._nconns - self.pool._num_pool > 0
        return False

    async def acquire(self) -> ConnectionProtocol:
        """Acquire connection from pool.
        """
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return await self.pool.getconn()

    async def release(self, conn: ConnectionProtocol) -> None:
        """Release connection to pool.
        """
        assert self.pool is not None, "Pool is not connected"
        await self.pool.putconn(conn)

    async def terminate(self) -> None:
        """Terminate all pool connections.
        """
        if self.pool is not None:
            await self.pool.close()

    @property
    def min_size(self) -> int:
        return self.pool.min_size

    @property
    def max_size(self) -> int:
        return self.pool.max_size


class MysqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool.
    """

    async def create(self) -> None:
        """Create connection pool asynchronously.
        """
        self.pool = cast(
            PoolProtocol,
            await aiomysql.create_pool(
                db=self.database, **self.connect_params
            ),
        )
