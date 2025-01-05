import logging
from contextlib import asynccontextmanager

import peewee
import uvicorn
from fastapi import FastAPI
import asyncio
from psycopg_pool import AsyncConnectionPool


import peewee_async


database = peewee_async.PsycopgDatabase(
    database='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port=5432,
    pool_params = { 
        "min_size": 0,
        "max_size": 4,
        "timeout": 5,
        "check": AsyncConnectionPool.check_connection
    }
)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("psycopg.pool").setLevel(logging.DEBUG)


class AppTestModel(peewee_async.AioModel):
    text = peewee.CharField(max_length=100)

    class Meta:
        database = database


@asynccontextmanager
async def lifespan(app: FastAPI):
    AppTestModel.drop_table()
    AppTestModel.create_table()
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
        await AppTestModel.aio_create(text="1")

        await asyncio.sleep(0.05)

        await AppTestModel.aio_create(text="2")


async def nested_atomic() -> None:
    async with database.aio_atomic():
        await AppTestModel.update(id=1).aio_execute()


@app.get("/savepoint")
async def savepoint():
    async with database.aio_atomic():
        await AppTestModel.update(id=1).aio_execute()
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
