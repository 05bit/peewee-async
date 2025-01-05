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
database = peewee_async.PooledPostgresqlDatabase(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432,
    max_connections=3
)



def setup_logging():
    logger = logging.getLogger("uvicorn.error")
    handler = logging.FileHandler(filename="app.log", mode="w")
    logger.addHandler(handler)


class MySimplestModel(peewee_async.AioModel):
    id = peewee.IntegerField(primary_key=True, sequence=True)

    class Meta:
        database = database


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.aio_execute_sql('CREATE TABLE IF NOT EXISTS MySimplestModel (id SERIAL PRIMARY KEY);')
    await database.aio_execute_sql('TRUNCATE TABLE MySimplestModel;')
    setup_logging()
    yield
    await database.aio_close()

app = FastAPI(lifespan=lifespan)
errors = set()


@app.get("/select")
async def select():
    try:
        await MySimplestModel.select().aio_execute()
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


async def nested_atomic():
    async with database.aio_atomic():
        await MySimplestModel.update(id=1).aio_execute()


@app.get("/transaction")
async def transaction():
    try:
        async with database.aio_atomic():
            await MySimplestModel.update(id=1).aio_execute()
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


@app.get("/atomic")
async def atomic():
    try:
        async with database.aio_atomic():
            await MySimplestModel.update(id=1).aio_execute()
            await nested_atomic()
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


@app.get("/recreate_pool")
async def atomic():
    await database.aio_close()
    await database.aio_connect()


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        access_log=True
    )
