import peewee
import asyncio

RESULTS_NAIVE = peewee.RESULTS_NAIVE
RESULTS_MODELS = peewee.RESULTS_MODELS
RESULTS_TUPLES = peewee.RESULTS_TUPLES
RESULTS_DICTS = peewee.RESULTS_DICTS
RESULTS_AGGREGATE_MODELS = peewee.RESULTS_AGGREGATE_MODELS


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
        self._initialized = False
        self._cursor = cursor
        self._rows = []
        self._result_cache = None
        self._result_wrapper = self._get_result_wrapper(query)

    def __iter__(self):
        while True:
            yield self._result_wrapper.iterate()

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
        db = query.database
        if query._tuples:
            QRW = db.get_result_wrapper(RESULTS_TUPLES)
        elif query._dicts:
            QRW = db.get_result_wrapper(RESULTS_DICTS)
        elif query._naive or not query._joins or query.verify_naive():
            QRW = db.get_result_wrapper(RESULTS_NAIVE)
        elif query._aggregate_rows:
            QRW = db.get_result_wrapper(RESULTS_AGGREGATE_MODELS)
        else:
            QRW = db.get_result_wrapper(RESULTS_MODELS)

        cursor = RowsCursor(self._rows, self._cursor.description)
        return QRW(query.model_class, cursor, query.get_query_meta())

    @asyncio.coroutine
    def fetchone(self):
        row = yield from self._cursor.fetchone()
        if not row:
            raise GeneratorExit
        self._rows.append(row)


class AsyncRawQueryWrapper(AsyncQueryWrapper):
    def _get_result_wrapper(self, query):
        """Get raw query result wrapper class.
        """
        db = query.database
        if query._tuples:
            QRW = db.get_result_wrapper(RESULTS_TUPLES)
        elif query._dicts:
            QRW = db.get_result_wrapper(RESULTS_DICTS)
        else:
            QRW = db.get_result_wrapper(RESULTS_NAIVE)

        cursor = RowsCursor(self._rows, self._cursor.description)
        return QRW(query.model_class, cursor, None)