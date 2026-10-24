import logging
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

import peewee as pw

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


try:
    import aiosqlite
except ImportError:
    aiosqlite = None  # type: ignore

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

    async def close(self) -> None: ...


def format_dsn(
    protocol: str,
    host: str,
    port: str | int,
    user: str,
    password: str,
    path: str = "",
    target_session_attrs: str | None = None,
) -> str:
    hosts = []
    if len(host.split(",")) != len(str(port).split(",")):
        raise ValueError("Number of hosts and ports must be the same")
    for index, h in enumerate(host.split(",")):
        p = str(port).split(",")[index]
        hosts.append(f"{h}:{p}")

    dns = f"{protocol}://{user}:{password}@{','.join(hosts)}/{path}"
    if target_session_attrs:
        dns += f"?target_session_attrs={target_session_attrs}"
    return dns


class ModuleRequired(Exception):
    def __init__(self, package: str) -> None:
        self.package = package
        self.message = f"{package} is not installed"
        super().__init__(self.message)


sort_models = pw.sort_models  # type: ignore[attr-defined]
database_required = pw.database_required  # type: ignore[attr-defined]
ensure_tuple = pw.ensure_tuple  # type: ignore[attr-defined]
