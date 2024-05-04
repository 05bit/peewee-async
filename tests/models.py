import uuid

import peewee
import peewee_async


class TestModel(peewee_async.AioModel):
    __test__ = False  # disable pytest warnings
    text = peewee.CharField(max_length=100, unique=True)
    data = peewee.TextField(default='')

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelAlpha(peewee_async.AioModel):
    __test__ = False
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelBeta(peewee_async.AioModel):
    __test__ = False
    alpha = peewee.ForeignKeyField(TestModelAlpha, backref='betas')
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class TestModelGamma(peewee_async.AioModel):
    __test__ = False
    text = peewee.CharField()
    beta = peewee.ForeignKeyField(TestModelBeta, backref='gammas')

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class UUIDTestModel(peewee_async.AioModel):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField()

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class CompatTestModel(peewee.Model):
    id = peewee.UUIDField(primary_key=True, default=uuid.uuid4)
    text = peewee.CharField(max_length=100, unique=True)
    data = peewee.TextField(default='')

    def __str__(self):
        return '<%s id=%s> %s' % (self.__class__.__name__, self.id, self.text)


class CompositeTestModel(peewee_async.AioModel):
    """A simple "through" table for many-to-many relationship."""
    uuid = peewee.ForeignKeyField(UUIDTestModel)
    alpha = peewee.ForeignKeyField(TestModelAlpha)

    class Meta:
        primary_key = peewee.CompositeKey('uuid', 'alpha')


ALL_MODELS = (
    TestModel, UUIDTestModel, TestModelAlpha, TestModelBeta, TestModelGamma,
    CompatTestModel, CompositeTestModel
)
