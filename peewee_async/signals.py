"""
Provide django-style hooks for model events.
"""
from peewee_async import AioModel as _Model


class Signal(object):
    def __init__(self):
        self._flush()

    def _flush(self):
        self._receivers = set()
        self._receiver_list = []

    def connect(self, receiver, name=None, sender=None):
        name = name or receiver.__name__
        key = (name, sender)
        if key not in self._receivers:
            self._receivers.add(key)
            self._receiver_list.append((name, receiver, sender))
        else:
            raise ValueError('receiver named %s (for sender=%s) already '
                             'connected' % (name, sender or 'any'))

    def disconnect(self, receiver=None, name=None, sender=None):
        if receiver:
            name = name or receiver.__name__
        if not name:
            raise ValueError('a receiver or a name must be provided')

        key = (name, sender)
        if key not in self._receivers:
            raise ValueError('receiver named %s for sender=%s not found.' %
                             (name, sender or 'any'))

        self._receivers.remove(key)
        self._receiver_list = [(n, r, s) for n, r, s in self._receiver_list
                               if (n, s) != key]

    def __call__(self, name=None, sender=None):
        def decorator(fn):
            self.connect(fn, name, sender)
            return fn
        return decorator

    async def send(self, instance, *args, **kwargs):
        sender = type(instance)
        responses = []
        for n, r, s in self._receiver_list:
            if s is None or isinstance(instance, s):
                responses.append((r, await r(sender, instance, *args, **kwargs)))
        return responses


aio_pre_save = Signal()
aio_post_save = Signal()
aio_pre_delete = Signal()
aio_post_delete = Signal()
pre_init = Signal() # can't be async !


class AioModel(_Model):

    def __init__(self, *args, **kwargs):
        super(AioModel, self).__init__(*args, **kwargs)
        pre_init.send(self)

    async def aio_save(self, *args, **kwargs):
        pk_value = self._pk if self._meta.primary_key else True
        created = kwargs.get('force_insert', False) or not bool(pk_value)
        await aio_pre_save.send(self, created=created)
        ret = await super(AioModel, self).aio_save(*args, **kwargs)
        await aio_post_save.send(self, created=created)
        return ret

    async def aio_delete_instance(self, *args, **kwargs):
        await aio_pre_delete.send(self)
        ret = await super(AioModel, self).aio_delete_instance(*args, **kwargs)
        await aio_post_delete.send(self)
        return ret
