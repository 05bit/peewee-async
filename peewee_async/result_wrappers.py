from typing import Any, List
from typing import Optional, Sequence

from peewee import BaseQuery

from .utils import CursorProtocol


class SyncCursorAdapter(object):
    def __init__(self, rows: List[Any], description: Optional[Sequence[Any]]) -> None:
        self._rows = rows
        self.description = description
        self._idx = 0

    def fetchone(self) -> Any:
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def close(self) -> None:
        pass


async def fetch_models(cursor: CursorProtocol, query: BaseQuery) -> List[Any]:
    rows = await cursor.fetchall()
    sync_cursor = SyncCursorAdapter(rows, cursor.description)
    _result_wrapper = query._get_cursor_wrapper(sync_cursor)
    return list(_result_wrapper)
