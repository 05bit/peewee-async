from peewee_async import AioModel as _Model
from typing import Union, Literal, Any
from playhouse.signals import Signal

class AioSignal(Signal):
    async def send(self, instance: "AioModel", *args: Any, **kwargs: Any) -> list[tuple[Any, Any]]:
        sender = type(instance)
        responses = []
        for n, r, s in self._receiver_list:
            if s is None or isinstance(instance, s):
                responses.append((r, await r(sender, instance, *args, **kwargs)))
        return responses


aio_pre_save = AioSignal()
aio_post_save = AioSignal()
aio_pre_delete = AioSignal()
aio_post_delete = AioSignal()
pre_init = Signal() # can't be async !


class AioModel(_Model):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(AioModel, self).__init__(*args, **kwargs)
        pre_init.send(self)

    async def aio_save(self, force_insert: bool = False, only: Any = None) -> Union[int, Literal[False]]:
        pk_value = self._pk if self._meta.primary_key else True
        created = force_insert or not bool(pk_value)
        await aio_pre_save.send(self, created=created)
        ret = await super(AioModel, self).aio_save(force_insert, only)
        await aio_post_save.send(self, created=created)
        return ret

    async def aio_delete_instance(self, recursive: bool = False, delete_nullable: bool = False) -> int:
        await aio_pre_delete.send(self)
        ret = await super(AioModel, self).aio_delete_instance(recursive, delete_nullable)
        await aio_post_delete.send(self)
        return ret
