import peewee_async
import peewee_asyncext


PG_DEFAULTS = {
    'database': 'postgres',
    'host': '127.0.0.1',
    'port': 5432,
    'password': 'postgres',
    'user': 'postgres',
}

MYSQL_DEFAULTS = {
    'database': 'mysql',
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'mysql'
}

DB_DEFAULTS = {
    'postgres': PG_DEFAULTS,
    'postgres-ext': PG_DEFAULTS,
    'postgres-pool': PG_DEFAULTS,
    'postgres-pool-ext': PG_DEFAULTS,
    'mysql': MYSQL_DEFAULTS,
    'mysql-pool': MYSQL_DEFAULTS
}
DB_CLASSES = {
    'postgres': peewee_async.PostgresqlDatabase,
    'postgres-ext': peewee_asyncext.PostgresqlExtDatabase,
    'postgres-pool': peewee_async.PooledPostgresqlDatabase,
    'postgres-pool-ext': peewee_asyncext.PooledPostgresqlExtDatabase,
    'mysql': peewee_async.MySQLDatabase,
    'mysql-pool': peewee_async.PooledMySQLDatabase
}
