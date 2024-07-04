import uuid


class Transaction:

    def __init__(self, connection, is_savepoint=False):
        self.connection = connection
        if is_savepoint:
            self.savepoint = f"PWASYNC__{uuid.uuid4().hex}"
        else:
            self.savepoint = None

    @property
    def is_savepoint(self):
        return self.savepoint is not None

    async def execute(self, sql):
        async with self.connection.cursor() as cursor:
            await cursor.execute(sql)

    async def begin(self):
        sql = "BEGIN"
        if self.savepoint:
            sql = f"SAVEPOINT {self.savepoint}"
        return await self.execute(sql)

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def commit(self):

        sql = "COMMIT"
        if self.savepoint:
            sql = f"RELEASE SAVEPOINT {self.savepoint}"
        return await self.execute(sql)

    async def rollback(self):
        sql = "ROLLBACK"
        if self.savepoint:
            sql = f"ROLLBACK TO SAVEPOINT {self.savepoint}"
        return await self.execute(sql)
