import uuid
from types import TracebackType
from typing import Optional, Type

from .utils import ConnectionProtocol


class Transaction:

    def __init__(self, connection: ConnectionProtocol, is_savepoint: bool = False):
        self.connection = connection
        self.savepoint: Optional[str] = None
        if is_savepoint:
            self.savepoint = f"PWASYNC__{uuid.uuid4().hex}"

    @property
    def is_savepoint(self) -> bool:
        return self.savepoint is not None

    async def execute(self, sql: str) -> None:
        async with self.connection.cursor() as cursor:
            await cursor.execute(sql)

    async def begin(self) -> None:
        sql = "BEGIN"
        if self.savepoint:
            sql = f"SAVEPOINT {self.savepoint}"
        await self.execute(sql)

    async def __aenter__(self) -> 'Transaction':
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType]
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def commit(self) -> None:

        sql = "COMMIT"
        if self.savepoint:
            sql = f"RELEASE SAVEPOINT {self.savepoint}"
        await self.execute(sql)

    async def rollback(self) -> None:
        sql = "ROLLBACK"
        if self.savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {self.savepoint}"
        await self.execute(sql)
