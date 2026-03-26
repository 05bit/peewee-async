import os

import peewee_async

PG_DEFAULTS = {
    "database": "postgres",
    "host": "127.0.0.1",
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "password": "postgres",
    "user": "postgres",
    "pool_params": {"minsize": 0, "maxsize": 5, "timeout": 30, "pool_recycle": 1.5},
}

PSYCOPG_DEFAULTS = {
    "database": "postgres",
    "host": "127.0.0.1",
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "password": "postgres",
    "user": "postgres",
    "pool_params": {"min_size": 0, "max_size": 5, "max_lifetime": 15},
}

MYSQL_DEFAULTS = {
    "database": "mysql",
    "host": "127.0.0.1",
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": "root",
    "password": "mysql",
    "connect_timeout": 30,
    "pool_params": {"minsize": 0, "maxsize": 5, "pool_recycle": 2},
}

SQLITE_DEFAULTS = {
    "database": "test.sqlite",
}

AIOPG_POOL = "aiopg-pool"
PSYCOPG_POOL = "psycopg-pool"
MYSQL_POOL = "mysql-pool"
SQLITE_POOL = "sqlite-pool"


DB_DEFAULTS = {
    AIOPG_POOL: PG_DEFAULTS,
    PSYCOPG_POOL: PSYCOPG_DEFAULTS,
    MYSQL_POOL: MYSQL_DEFAULTS,
    SQLITE_POOL: SQLITE_DEFAULTS,
}

DB_CLASSES = {
    AIOPG_POOL: peewee_async.PostgresqlDatabase,
    PSYCOPG_POOL: peewee_async.Psycopg3Database,
    MYSQL_POOL: peewee_async.MySQLDatabase,
    SQLITE_POOL: peewee_async.SqliteDatabase
}
