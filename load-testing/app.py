import logging
import random
from contextlib import asynccontextmanager

import peewee
import uvicorn
from aiopg.connection import Connection
from aiopg.pool import Pool
from fastapi import FastAPI

acquire = Pool.acquire
cursor = Connection.cursor


def new_acquire(self):
    choice = random.randint(1, 5)
    if choice == 5:
        raise Exception("some network error")  # network error imitation
    return acquire(self)


def new_cursor(self):
    choice = random.randint(1, 5)
    if choice == 5:
        raise Exception("some network error")  # network error imitation
    return cursor(self)

Connection.cursor = new_cursor
Pool.acquire = new_acquire

import peewee_async


logging.basicConfig()
pg_db = peewee_async.PooledPostgresqlDatabase(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432,
    max_connections=3
)


class Manager(peewee_async.Manager):
    """Async models manager."""

    database = pg_db


manager = Manager()


def setup_logging():
    logger = logging.getLogger("uvicorn.error")
    handler = logging.FileHandler(filename="app.log", mode="w")
    logger.addHandler(handler)


class MySimplestModel(peewee.Model):
    id = peewee.IntegerField(primary_key=True, sequence=True)

    class Meta:
        database = pg_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await peewee_async._run_no_result_sql(  # noqa
        database=manager.database,
        operation='CREATE TABLE IF NOT EXISTS MySimplestModel (id SERIAL PRIMARY KEY);',
    )
    await peewee_async._run_no_result_sql(  # noqa
        database=manager.database,
        operation='TRUNCATE TABLE MySimplestModel;',
    )
    setup_logging()
    yield
    # Clean up the ML models and release the resources
    await manager.close()


app = FastAPI(lifespan=lifespan)
errors = set()


@app.get("/select")
async def select():
    try:
        await manager.execute(MySimplestModel.select())
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


async def nested_transaction():
    async with manager.transaction():
        await manager.execute(MySimplestModel.update(id=1))


async def nested_atomic():
    async with manager.atomic():
        await manager.execute(MySimplestModel.update(id=1))


@app.get("/transaction")
async def transaction():
    try:
        async with manager.transaction():
            await manager.execute(MySimplestModel.update(id=1))
            await nested_transaction()
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


@app.get("/atomic")
async def atomic():
    try:
        async with manager.atomic():
            await manager.execute(MySimplestModel.update(id=1))
            await nested_atomic()
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


@app.get("/recreate_pool")
async def atomic():
    await manager.database.close_async()
    await manager.database.connect_async()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        access_log=True
    )
