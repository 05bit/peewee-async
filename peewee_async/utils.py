import logging

try:
    import aiopg
    import psycopg2
except ImportError:
    aiopg = None
    psycopg2 = None

try:
    import aiomysql
    import pymysql
except ImportError:
    aiomysql = None
    pymysql = None

__log__ = logging.getLogger('peewee.async')
__log__.addHandler(logging.NullHandler())