import abc
import asyncio
from typing import Any, cast

from .utils import ConnectionProtocol, ModuleRequired, aiomysql, aiopg, aiosqlite, format_dsn, psycopg, psycopg_pool


class PoolBackend(metaclass=abc.ABCMeta):
    """Asynchronous database connection pool."""

    required_modules: list[str] = []

    def __init__(self, *, database: str, **kwargs: Any) -> None:
        self.check_required_backend()
        self.pool: Any | None = None
        self.database = database
        self.connect_params = kwargs
        self._connection_lock = asyncio.Lock()

    def check_required_backend(self) -> None:
        for module in self.required_modules:
            try:
                __import__(module)
            except ImportError:
                raise ModuleRequired(module) from None

    @property
    def is_connected(self) -> bool:
        if self.pool is not None:
            return self.pool.closed is False
        return False

    @abc.abstractmethod
    def has_acquired_connections(self) -> bool:
        """Checks if the pool has acquired connections"""
        ...

    async def connect(self) -> None:
        async with self._connection_lock:
            if self.is_connected is False:
                await self.create()

    @abc.abstractmethod
    async def acquire(self) -> ConnectionProtocol:
        """Acquire connection from the pool."""
        ...

    @abc.abstractmethod
    async def release(self, conn: ConnectionProtocol) -> None:
        """Release connection to the pool."""
        ...

    @abc.abstractmethod
    async def create(self) -> None:
        """Create connection pool asynchronously."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the pool."""
        ...


class AioPgPoolBackend(PoolBackend):
    """Asynchronous database connection pool based on aiopg."""

    required_modules = ["aiopg"]

    async def create(self) -> None:
        if "connect_timeout" in self.connect_params:
            self.connect_params["timeout"] = self.connect_params.pop("connect_timeout")
        self.pool = await aiopg.create_pool(database=self.database, **self.connect_params)

    async def acquire(self) -> ConnectionProtocol:
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return cast("ConnectionProtocol", await self.pool.acquire())

    async def release(self, conn: ConnectionProtocol) -> None:
        assert self.pool is not None, "Pool is not connected"
        self.pool.release(conn)

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.terminate()
            await self.pool.wait_closed()

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return len(self.pool._used) > 0
        return False


class PsycopgPoolBackend(PoolBackend):
    """Asynchronous database connection pool based on psycopg + psycopg_pool."""

    required_modules = ["psycopg", "psycopg_pool"]

    async def create(self) -> None:
        params = self.connect_params.copy()
        pool = psycopg_pool.AsyncConnectionPool(
            format_dsn(
                "postgresql",
                host=params.pop("host"),
                port=params.pop("port"),
                user=params.pop("user"),
                password=params.pop("password"),
                path=self.database,
            ),
            kwargs={
                "cursor_factory": psycopg.AsyncClientCursor,
                "autocommit": True,
            },
            open=False,
            **params,
        )

        await pool.open()
        self.pool = pool

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            stats = self.pool.get_stats()
            return stats["pool_size"] > stats["pool_available"]  # type: ignore
        return False

    async def acquire(self) -> ConnectionProtocol:
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return cast("ConnectionProtocol", await self.pool.getconn())

    async def release(self, conn: ConnectionProtocol) -> None:
        assert self.pool is not None, "Pool is not connected"
        await self.pool.putconn(conn)

    async def close(self) -> None:
        """Close the pool. Notes the pool does not close active connections"""
        if self.pool is not None:
            await self.pool.close()


class AioMysqlPoolBackend(PoolBackend):
    """Asynchronous database connection pool based on aiomysql."""

    required_modules = ["aiomysql"]

    async def create(self) -> None:
        self.pool = await aiomysql.create_pool(db=self.database, **self.connect_params)

    async def acquire(self) -> ConnectionProtocol:
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return cast("ConnectionProtocol", await self.pool.acquire())

    async def release(self, conn: ConnectionProtocol) -> None:
        assert self.pool is not None, "Pool is not connected"
        self.pool.release(conn)

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return len(self.pool._used) > 0
        return False

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.terminate()
            await self.pool.wait_closed()


class AioSqlitePool:
    def __init__(self, database: str, **connect_params: Any) -> None:
        self._opened_connections: set[ConnectionProtocol] = set()
        self.database = database
        self.connect_params = connect_params
        self._closed = False

    async def acquire(self) -> ConnectionProtocol:
        if self._closed:
            raise RuntimeError("Cannot acquire connection after closing pool")
        return cast(
            "ConnectionProtocol",
            await aiosqlite.connect(database=self.database, isolation_level=None, **self.connect_params),
        )

    async def release(self, conn: ConnectionProtocol) -> None:
        await conn.close()

    def has_acquired_connections(self) -> bool:
        return len(self._opened_connections) > 0

    async def close(self) -> None:
        for c in self._opened_connections:
            await self.release(c)
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class AioSqlitePoolBackend(PoolBackend):
    """Asynchronous database connection pool based on aiosqlite."""

    required_modules = ["aiosqlite"]

    async def create(self) -> None:
        self.pool: AioSqlitePool = AioSqlitePool(database=self.database, **self.connect_params)

    async def acquire(self) -> ConnectionProtocol:
        if self.pool is None:
            await self.connect()
        assert self.pool is not None, "Pool is not connected"
        return await self.pool.acquire()

    async def release(self, conn: ConnectionProtocol) -> None:
        assert self.pool is not None, "Pool is not connected"
        await self.pool.release(conn)

    def has_acquired_connections(self) -> bool:
        if self.pool is not None:
            return self.pool.has_acquired_connections()
        return False

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
