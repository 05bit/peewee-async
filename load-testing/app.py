import peewee
from fastapi import FastAPI
import logging
import uvicorn
import random

import peewee_async
from contextlib import asynccontextmanager
import functools

logging.basicConfig()
pg_db = peewee_async.PooledPostgresqlDatabase(None)


class Manager(peewee_async.Manager):
    """Async models manager."""

    database = peewee_async.PooledPostgresqlDatabase(
        database='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port=5432,
    )


manager = Manager()


def patch_manager(manager):
    async def cursor(self, conn=None, *args, **kwargs):

        choice = random.randint(1, 5)
        if choice == 5:
            raise Exception("some network error")  # network error imitation

        # actual code
        in_transaction = conn is not None
        if not conn:
            conn = await self.acquire()
        cursor = await conn.cursor(*args, **kwargs)
        cursor.release = functools.partial(
            self.release_cursor, cursor,
            in_transaction=in_transaction)
        return cursor

    manager.database._async_conn_cls.cursor = cursor

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
    patch_manager(manager)
    yield
    # Clean up the ML models and release the resources
    await manager.close()


app = FastAPI(lifespan=lifespan)
errors = set()


@app.get("/select")
async def test():
    try:
        await manager.execute(MySimplestModel.select())
    except Exception as e:
        errors.add(str(e))
        raise
    return errors


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        access_log=True
    )
