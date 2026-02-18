import uuid

import peewee as pw

import peewee_async
import peewee_async.signals


class TestModel(peewee_async.AioModel):
    __test__ = False  # disable pytest warnings
    text = pw.CharField(max_length=100, unique=True)
    data = pw.TextField(default="")

    def __str__(self) -> str:
        return "<%s id=%s> %s" % (self.__class__.__name__, self.id, self.text)


class TestModelAlpha(peewee_async.AioModel):
    __test__ = False
    text = pw.CharField()

    def __str__(self) -> str:
        return "<%s id=%s> %s" % (self.__class__.__name__, self.id, self.text)


class TestModelBeta(peewee_async.AioModel):
    __test__ = False
    alpha = pw.ForeignKeyField(TestModelAlpha, backref="betas")
    text = pw.CharField()

    def __str__(self) -> str:
        return "<%s id=%s> %s" % (self.__class__.__name__, self.id, self.text)


class TestModelGamma(peewee_async.AioModel):
    __test__ = False
    text = pw.CharField()
    beta = pw.ForeignKeyField(TestModelBeta, backref="gammas")

    def __str__(self) -> str:
        return "<%s id=%s> %s" % (self.__class__.__name__, self.id, self.text)


class UUIDTestModel(peewee_async.AioModel):
    id = pw.UUIDField(primary_key=True, default=uuid.uuid4)
    text = pw.CharField()

    def __str__(self) -> str:
        return "<%s id=%s> %s" % (self.__class__.__name__, self.id, self.text)


class CompositeTestModel(peewee_async.AioModel):
    """A simple "through" table for many-to-many relationship."""

    task_id = pw.IntegerField()
    product_type = pw.CharField()

    class Meta:
        primary_key = pw.CompositeKey("task_id", "product_type")


class IntegerTestModel(peewee_async.AioModel):
    __test__ = False  # disable pytest warnings
    num = pw.IntegerField()


class TestSignalModel(peewee_async.signals.AioModel):
    __test__ = False  # disable pytest warnings
    text = pw.CharField(max_length=100)


ALL_MODELS = (
    TestModel,
    UUIDTestModel,
    TestModelAlpha,
    TestModelBeta,
    TestModelGamma,
    CompositeTestModel,
    IntegerTestModel,
    TestSignalModel,
)
