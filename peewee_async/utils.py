import logging
from typing import Any, Protocol, Optional, Sequence, Set, AsyncContextManager

try:
    import aiopg
    import psycopg2
except ImportError:
    aiopg = None
    psycopg2 = None

try:
    import aiomysql
    import pymysql
except ImportError:
    aiomysql = None
    pymysql = None

__log__ = logging.getLogger('peewee.async')
__log__.addHandler(logging.NullHandler())


class CursorProtocol(Protocol):
    async def fetchone(self) -> Any:
        ...

    @property
    def lastrowid(self) -> int:
        ...

    @property
    def description(self) -> Optional[Sequence[Any]]:
        ...

    @property
    def rowcount(self) -> int:
        ...

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> None:
        ...


class ConnectionProtocol(Protocol):
    def cursor(
        self,
        **kwargs: Any
    ) -> AsyncContextManager[CursorProtocol]:
        ...


class PoolProtocol(Protocol):

    _used: Set[ConnectionProtocol]

    @property
    def closed(self) -> bool:
        ...

    async def acquire(self) -> ConnectionProtocol:
        ...

    def release(self, conn: ConnectionProtocol) -> None:
        ...

    def terminate(self) -> None:
        ...

    async def wait_closed(self) -> None:
        ...

