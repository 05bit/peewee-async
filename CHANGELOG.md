# Changelog

## 0.7.0 (latest)

- Switching from alpha to main release version number
- #66 pull request is merged, allow to pass timeout database connection

## 0.6.0a

- #97 pull request is merged, initial implementation of peewee 3.5+ support thanks to @Koos85
- Dropped support of peewee <= 3.5 and Python <= 3.5
- Rewritten to async / await syntax
- Closing issues with peewee incompatibility and other stuff: #95, #88, #78, #63, #62

## 0.5.12

- #92 pull request is merged, raise original exception instead of ValueError in atomic on disconnect

## 0.5.11

- #86 pull request is merged, releasing cursor issue after transaction releated queries

## 0.5.10

- #77, get object by index from `select()` result should work again 

## 0.5.9

- Get rid of `limit(1)` in `get()`
- Tests for aggregated queries

## 0.5.8

- #10, aggregated rows seems to work now (no tests yet, sorry)

## 0.5.7

- #49, compatibility with peewee 2.8.6+
- Fix: #50, NameError in `_swap_database` method

## 0.5.6

- Feature #32: debug logging to `peewee.async` logger
- Fix #46: another issue with `Proxy` database
- Fix #44: argument names now should not clash with model field names

## 0.5.5

- Fix: #34, speedups for result wrappers are not supported

## 0.5.4

- Fix: #30 and perform some internal cleanups
- Database `.allow_sync` attribute is deprecated, `allow_sync()` context manager or `.set_allow_sync()` should be used instad

## 0.5.3

- Fix: #26, closing MySQL connection and general implementation improvements
- #28 (pull request): better proxy database support in `Manager`

## 0.5.2

- Fix #24: prevent stucking after connection error
- Fix #25: starting transaction before connecting raise `AttributeError` exception about `_task_data`
- Removed `tasklocals` package from dependencies
- **Require** transactions to be run within task context

## 0.5.1

- Fix: #23, running not in the context of a task
- Automatically wrap transaction in task if not running task already

## 0.5.0

- Add high-level API via Manager class
- Add support for MySQL
- Auto-connect is performed for async queries if database is initialized, so no need to call `connect_async()` manually! And no worries, nothing will happen on duplicate calls
- Run SQL within `peewee.Database.exception_wrapper()` context, the same way as for sync requests

## 0.4.1

- Fixing critical transactions issues, see #12
- Internal `SELECT` query executor rewritten, got rid of some hacks
- Deferred database init is supported

## 0.4.0

- Add `db.atomic_async()` context manager to support transactions. Thanks, @mrbox!

## 0.3.4

- Fix: cursor is released back to connection pool after SQL execution error

## 0.3.3

- Add public `allow_sync` flag to database class, `True` by default
- Remove arguments from `sync_unwanted()` context manager function
- Add autodocs on `peewee_asyncext`

## 0.3

- #7, fixed bug with empty result after inserting row with UUID pk 
- some missing tests added
