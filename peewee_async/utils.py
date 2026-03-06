import logging
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

try:
    import aiopg
    import psycopg2
except ImportError:
    aiopg = None  # type: ignore
    psycopg2 = None

try:
    import psycopg
    import psycopg_pool
except ImportError:
    psycopg = None  # type: ignore
    psycopg_pool = None  # type: ignore

try:
    import aiomysql
    import pymysql
except ImportError:
    aiomysql = None
    pymysql = None  # type: ignore

__log__ = logging.getLogger("peewee.async")
__log__.addHandler(logging.NullHandler())


class CursorProtocol(Protocol):
    async def fetchone(self) -> Any: ...

    async def fetchall(self) -> list[Any]: ...

    async def fetchmany(self, size: int) -> list[Any]: ...

    @property
    def lastrowid(self) -> int: ...

    @property
    def description(self) -> Sequence[Any] | None: ...

    @property
    def rowcount(self) -> int: ...

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> None: ...


class ConnectionProtocol(Protocol):
    def cursor(self, **kwargs: Any) -> AbstractAsyncContextManager[CursorProtocol]: ...


FetchResults = Callable[[CursorProtocol], Awaitable[Any]]


def format_dsn(protocol: str, host: str, port: str | int, user: str, password: str, path: str = "") -> str:
    return f"{protocol}://{user}:{password}@{host}:{port}/{path}"
