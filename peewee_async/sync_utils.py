import contextlib
import warnings

@contextlib.contextmanager
def sync_unwanted(database):
    """Context manager for preventing unwanted sync queries.
    `UnwantedSyncQueryError` exception will raise on such query.

    NOTE: sync_unwanted() context manager is **deprecated**, use
    database's `.allow_sync()` context manager or `Manager.allow_sync()`
    context manager.
    """
    warnings.warn("sync_unwanted() context manager is deprecated, "
                  "use database's `.allow_sync()` context manager or "
                  "`Manager.allow_sync()` context manager. ",
                  DeprecationWarning)
    old_allow_sync = database._allow_sync
    database._allow_sync = False
    yield
    database._allow_sync = old_allow_sync


class UnwantedSyncQueryError(Exception):
    """Exception which is raised when performing unwanted sync query.

    NOTE: UnwantedSyncQueryError is deprecated, `assert` is used instead.
    """
    def __init__(self, *args, **kwargs):
        warnings.warn("UnwantedSyncQueryError is deprecated, "
                      "assert is used instead.",
                      DeprecationWarning)