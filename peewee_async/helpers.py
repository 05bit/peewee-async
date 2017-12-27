import peewee
import asyncio
from .utils import logger

def _get_exception_wrapper(database):
    """Get peewee exceptions context manager for database
    in backward compatible manner.
    """
    if isinstance(database.exception_wrapper, peewee.ExceptionWrapper):
        return database.exception_wrapper
    else:
        return database.exception_wrapper()


@asyncio.coroutine
def _run_sql(database, operation, *args, **kwargs):
    """Run SQL operation (query or command) against database.
    """
    logger.debug((operation, args, kwargs))

    with _get_exception_wrapper(database):
        cursor = yield from database.cursor_async()

        try:
            yield from cursor.execute(operation, *args, **kwargs)
        except:
            yield from cursor.release
            raise

        return cursor


@asyncio.coroutine
def _execute_query_async(query):
    """Execute query and return cursor object.
    """
    return (yield from _run_sql(query.database, *query.sql()))


class TaskLocals:
    """Simple `dict` wrapper to get and set values on per `asyncio`
    task basis.

    The idea is similar to thread-local data, but actually *much* simpler.
    It's no more than a "sugar" class. Use `get()` and `set()` method like
    you would to for `dict` but values will be get and set in the context
    of currently running `asyncio` task.

    When task is done, all saved values is removed from stored data.
    """
    def __init__(self, loop):
        self.loop = loop
        self.data = {}

    def get(self, key, *val):
        """Get value stored for current running task. Optionally
        you may provide the default value. Raises `KeyError` when
        can't get the value and no default one is provided.
        """
        data = self.get_data()
        if data is not None:
            return data.get(key, *val)
        elif len(val):
            return val[0]
        else:
            raise KeyError(key)

    def set(self, key, val):
        """Set value stored for current running task.
        """
        data = self.get_data(True)
        if data is not None:
            data[key] = val
        else:
            raise RuntimeError("No task is currently running")

    def get_data(self, create=False):
        """Get dict stored for current running task. Return `None`
        or an empty dict if no data was found depending on the
        `create` argument value.

        :param create: if argument is `True`, create empty dict
                       for task, default: `False`
        """
        task = asyncio.Task.current_task(loop=self.loop)
        if task:
            task_id = id(task)
            if create and not task_id in self.data:
                self.data[task_id] = {}
                task.add_done_callback(self.del_data)
            return self.data.get(task_id)

    def del_data(self, task):
        """Delete data for task from stored data dict.
        """
        del self.data[id(task)]
