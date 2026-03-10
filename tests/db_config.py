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

DB_DEFAULTS = {
    "aiopg-pool": PG_DEFAULTS,
    "psycopg-pool": PSYCOPG_DEFAULTS,
    "mysql-pool": MYSQL_DEFAULTS,
}

DB_CLASSES = {
    "aiopg-pool": peewee_async.PostgresqlDatabase,
    "psycopg-pool": peewee_async.Psycopg3Database,
    "mysql-pool": peewee_async.MySQLDatabase,
}
