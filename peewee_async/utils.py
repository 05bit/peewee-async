import logging
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

try:
    import aiopg
except ImportError:
    aiopg = None  # type: ignore
try:
    import psycopg
    import psycopg_pool
except ImportError:
    psycopg = None  # type: ignore
    psycopg_pool = None  # type: ignore

try:
    import aiomysql
except ImportError:
    aiomysql = None

__log__ = logging.getLogger("peewee.async")
__log__.addHandler(logging.NullHandler())


class CursorProtocol(Protocol):
    async def fetchone(self) -> Sequence[Any]: ...

    async def fetchall(self) -> Sequence[Any]: ...

    async def fetchmany(self, size: int) -> Sequence[Any]: ...

    @property
    def lastrowid(self) -> int: ...

    @property
    def description(self) -> Sequence[Any] | None: ...

    @property
    def rowcount(self) -> int: ...

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> None: ...


class ConnectionProtocol(Protocol):
    def cursor(self, **kwargs: Any) -> AbstractAsyncContextManager[CursorProtocol]: ...


def format_dsn(protocol: str, host: str, port: str | int, user: str, password: str, path: str = "") -> str:
    return f"{protocol}://{user}:{password}@{host}:{port}/{path}"


class ModuleRequired(Exception):
    def __init__(self, package: str) -> None:
        self.package = package
        self.message = f"{package} is not installed"
        super().__init__(self.message)
