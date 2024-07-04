from contextvars import ContextVar
from typing import Optional


class ConnectionContext:
    def __init__(self, connection):
        self.connection = connection
        # needs for to know whether begin a transaction  or create a savepoint
        self.transaction_is_opened = False


connection_context: ContextVar[Optional[ConnectionContext]] = ContextVar("connection_context", default=None)


class ConnectionContextManager:
    def __init__(self, pool_backend):
        self.pool_backend = pool_backend
        self.connection_context = connection_context.get()
        self.resuing_connection = self.connection_context is not None

    async def __aenter__(self):
        if self.connection_context is not None:
            connection = self.connection_context.connection
        else:
            connection = await self.pool_backend.acquire()
            self.connection_context = ConnectionContext(connection)
            connection_context.set(self.connection_context)
        return connection

    async def __aexit__(self, *args):
        if self.resuing_connection is False:
            self.pool_backend.release(self.connection_context.connection)
            connection_context.set(None)
