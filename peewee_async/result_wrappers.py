class RowsCursor(object):
    def __init__(self, rows, description):
        self._rows = rows
        self.description = description
        self._idx = 0

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def close(self):
        pass


class AsyncQueryWrapper:
    """Async query results wrapper for async `select()`. Internally uses
    results wrapper produced by sync peewee select query.

    Arguments:

        result_wrapper -- empty results wrapper produced by sync `execute()`
        call cursor -- async cursor just executed query

    To retrieve results after async fetching just iterate over this class
    instance, like you generally iterate over sync results wrapper.
    """
    def __init__(self, *, cursor=None, query=None):
        self._cursor = cursor
        self._rows = []
        self._result_cache = None
        self._result_wrapper = self._get_result_wrapper(query)

    def __iter__(self):
        return iter(self._result_wrapper)

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, idx):
        # NOTE: side effects will appear when both
        # iterating and accessing by index!
        if self._result_cache is None:
            self._result_cache = list(self)
        return self._result_cache[idx]

    def _get_result_wrapper(self, query):
        """Get result wrapper class.
        """
        cursor = RowsCursor(self._rows, self._cursor.description)
        return query._get_cursor_wrapper(cursor)

    async def fetchone(self):
        """Fetch single row from the cursor.
        """
        row = await self._cursor.fetchone()
        if not row:
            raise GeneratorExit
        self._rows.append(row)

    async def fetchall(self):
        try:
            while True:
                await self.fetchone()
        except GeneratorExit:
            pass

    @classmethod
    async def make_for_all_rows(cls, cursor, query):
        result = AsyncQueryWrapper(cursor=cursor, query=query)
        await result.fetchall()
        return result
