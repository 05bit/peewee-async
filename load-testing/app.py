import logging
from contextlib import asynccontextmanager

import peewee
import uvicorn
from fastapi import FastAPI
import asyncio
from aiopg.connection import Connection
from aiopg.pool import Pool
import random


import peewee_async


aiopg_database = peewee_async.PooledPostgresqlDatabase(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432,
        pool_params = { 
        "minsize": 0,
        "maxsize": 3,
    }
)

psycopg_database = peewee_async.PsycopgDatabase(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432,
    pool_params = { 
        "min_size": 0,
        "max_size": 3,
    }
)

database = psycopg_database


def patch_aiopg():
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


def setup_logging():
    logging.basicConfig()
    # logging.getLogger("psycopg.pool").setLevel(logging.DEBUG)
    logger = logging.getLogger("uvicorn.error")
    handler = logging.FileHandler(filename="app.log", mode="w")
    logger.addHandler(handler)


class AppTestModel(peewee_async.AioModel):
    text = peewee.CharField(max_length=100)

    class Meta:
        database = database


@asynccontextmanager
async def lifespan(app: FastAPI):
    AppTestModel.drop_table()
    AppTestModel.create_table()
    await AppTestModel.aio_create(id=1, text="1")
    await AppTestModel.aio_create(id=2, text="2")
    setup_logging()
    yield
    await database.aio_close()

app = FastAPI(lifespan=lifespan)
errors = set()


@app.get("/errors")
async def select():
    return errors


@app.get("/select")
async def select():
    await AppTestModel.select().aio_execute()



@app.get("/transaction")
async def transaction() -> None:
    async with database.aio_atomic():
        await AppTestModel.update(text="5").where(AppTestModel.id==1).aio_execute()

        await asyncio.sleep(0.05)

        await AppTestModel.update(text="10").where(AppTestModel.id==1).aio_execute()


async def nested_atomic() -> None:
    async with database.aio_atomic():
        await AppTestModel.update(text="1").where(AppTestModel.id==1).aio_execute()


@app.get("/savepoint")
async def savepoint():
    async with database.aio_atomic():
        await AppTestModel.update(text="2").where(AppTestModel.id==2).aio_execute()
        await nested_atomic()



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
