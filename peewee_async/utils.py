import logging
from typing import TypeVar, Any, Protocol, Optional, Sequence

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
    def description(self) -> Optional[Sequence[Any]]:
        ...


T_Connection = TypeVar("T_Connection", bound=Any)
