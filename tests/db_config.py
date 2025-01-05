import os
import peewee_async

PG_DEFAULTS = {
    'database': 'postgres',
    'host': '127.0.0.1',
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'password': 'postgres',
    'user': 'postgres',
    'min_connections': 0,
    'max_connections': 5,
    'pool_params': {"timeout": 30, 'pool_recycle': 1.5}
}

PSYCOPG_DEFAULTS = {
    'database': 'postgres',
    'host': '127.0.0.1',
    'port': int(os.environ.get('POSTGRES_PORT', 5432)),
    'password': 'postgres',
    'user': 'postgres',
    'pool_params': {"min_size": 0, "max_size": 5, 'max_lifetime': 60 * 60.0}
}

MYSQL_DEFAULTS = {
    'database': 'mysql',
    'host': '127.0.0.1',
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'user': 'root',
    'password': 'mysql',
    'connect_timeout': 30,
    'min_connections': 0,
    'max_connections': 5,
    "pool_params": {"pool_recycle": 2}
}

DB_DEFAULTS = {
    'postgres-pool': PG_DEFAULTS,
    'postgres-pool-ext': PG_DEFAULTS,
    'psycopg-pool': PSYCOPG_DEFAULTS,
    'mysql-pool': MYSQL_DEFAULTS
}

DB_CLASSES = {
    'postgres-pool': peewee_async.PooledPostgresqlDatabase,
    'postgres-pool-ext': peewee_async.PooledPostgresqlExtDatabase,
    'psycopg-pool': peewee_async.PsycopgDatabase,
    'mysql-pool': peewee_async.PooledMySQLDatabase
}
