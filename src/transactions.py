import asyncio
import uuid

from . import utils


class transaction:
    """Asynchronous context manager (`async with`), similar to
    `peewee.transaction()`. Will start new `asyncio` task for
    transaction if not started already.
    """
    def __init__(self, db):
        self.db = db
        self.loop = db.loop

    @asyncio.coroutine
    def commit(self, begin=True):
        yield from utils._run_sql(self.db, 'COMMIT')
        if begin:
            yield from utils._run_sql(self.db, 'BEGIN')

    @asyncio.coroutine
    def rollback(self, begin=True):
        yield from utils._run_sql(self.db, 'ROLLBACK')
        if begin:
            yield from utils._run_sql(self.db, 'BEGIN')

    @asyncio.coroutine
    def __aenter__(self):
        if not asyncio.Task.current_task(loop=self.loop):
            raise RuntimeError("The transaction must run within a task")
        yield from self.db.push_transaction_async()
        if self.db.transaction_depth_async() == 1:
            yield from utils._run_sql(self.db, 'BEGIN')
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                yield from self.rollback(False)
            elif self.db.transaction_depth_async() == 1:
                try:
                    yield from self.commit(False)
                except:
                    yield from self.rollback(False)
                    raise
        finally:
            yield from self.db.pop_transaction_async()


class savepoint:
    """Asynchronous context manager (`async with`), similar to
    `peewee.savepoint()`.
    """
    def __init__(self, db, sid=None):
        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = db.compiler().quote(self.sid)

    @asyncio.coroutine
    def commit(self):
        yield from utils._run_sql(self.db, 'RELEASE SAVEPOINT %s;' % self.quoted_sid)

    @asyncio.coroutine
    def rollback(self):
        yield from utils._run_sql(self.db, 'ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    @asyncio.coroutine
    def __aenter__(self):
        yield from utils._run_sql(self.db, 'SAVEPOINT %s;' % self.quoted_sid)
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                yield from self.rollback()
            else:
                try:
                    yield from self.commit()
                except:
                    yield from self.rollback()
                    raise
        finally:
            pass


class atomic:
    """Asynchronous context manager (`async with`), similar to
    `peewee.atomic()`.
    """
    def __init__(self, db):
        self.db = db

    @asyncio.coroutine
    def __aenter__(self):
        if self.db.transaction_depth_async() > 0:
            self._ctx = self.db.savepoint_async()
        else:
            self._ctx = self.db.transaction_async()
        return (yield from self._ctx.__aenter__())

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        yield from self._ctx.__aexit__(exc_type, exc_val, exc_tb)
