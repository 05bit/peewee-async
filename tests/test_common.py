import uuid

from tests.conftest import all_dbs
from tests.models import TestModel


@all_dbs
async def test_get_or_none(manager):
    """Test get_or_none manager function."""
    text1 = "Test %s" % uuid.uuid4()
    text2 = "Test %s" % uuid.uuid4()

    obj1 = await manager.create(TestModel, text=text1)
    obj2 = await manager.get_or_none(TestModel, text=text1)
    obj3 = await manager.get_or_none(TestModel, text=text2)

    assert obj1 == obj2
    assert obj1 is not None
    assert obj2 is not None
    assert obj3 is None
