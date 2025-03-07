from typing import List, Union
import uuid

import peewee
import pytest
from peewee import fn

from peewee_async.databases import AioDatabase
from tests.conftest import dbs_all
from tests.models import TestModel, IntegerTestModel, TestModelAlpha, TestModelBeta, TestModelGamma


@dbs_all
async def test_aio_get(db: AioDatabase) -> None:
    obj1 = await TestModel.aio_create(text="Test 1")
    obj2 = await TestModel.aio_create(text="Test 2")

    result = await TestModel.aio_get(TestModel.id == obj1.id)
    assert result.id == obj1.id

    result = await TestModel.aio_get(TestModel.text == "Test 2")
    assert result.id == obj2.id

    with pytest.raises(TestModel.DoesNotExist):
        await TestModel.aio_get(TestModel.text == "unknown")


@dbs_all
async def test_aio_get_or_none(db: AioDatabase) -> None:
    obj1 = await TestModel.aio_create(text="Test 1")

    result = await TestModel.aio_get_or_none(TestModel.id == obj1.id)
    assert result is not None and result.id == obj1.id

    result = await TestModel.aio_get_or_none(TestModel.text == "unknown")
    assert result is None


@dbs_all
@pytest.mark.parametrize(
    ["peek_num", "expected"],
    (
        (1, 1),
        (2, [1,2]),
        (5, [1,2,3]),
    )
)
async def test_aio_peek(
    db: AioDatabase,
    peek_num: int,
    expected: Union[int, List[int]]
) -> None:
    await IntegerTestModel.aio_create(num=1)
    await IntegerTestModel.aio_create(num=2)
    await IntegerTestModel.aio_create(num=3)

    rows = await IntegerTestModel.select().order_by(
        IntegerTestModel.num
    ).aio_peek(n=peek_num)

    if isinstance(rows, list):
        result = [r.num for r in rows]
    else:
        result = rows.num
    assert result == expected


@dbs_all
@pytest.mark.parametrize(
    ["first_num", "expected"],
    (
        (1, 1),
        (2, [1,2]),
        (5, [1,2,3]),
    )
)
async def test_aio_first(
    db: AioDatabase,
    first_num: int,
    expected: Union[int, List[int]]
) -> None:
    await IntegerTestModel.aio_create(num=1)
    await IntegerTestModel.aio_create(num=2)
    await IntegerTestModel.aio_create(num=3)

    rows = await IntegerTestModel.select().order_by(
        IntegerTestModel.num
    ).aio_first(n=first_num)

    if isinstance(rows, list):
        result = [r.num for r in rows]
    else:
        result = rows.num
    assert result == expected


@dbs_all
async def test_aio_scalar(db: AioDatabase) -> None:
    await IntegerTestModel.aio_create(num=1)
    await IntegerTestModel.aio_create(num=2)

    assert await IntegerTestModel.select(fn.MAX(IntegerTestModel.num)).aio_scalar() == 2

    assert await IntegerTestModel.select(
        fn.MAX(IntegerTestModel.num),fn.Min(IntegerTestModel.num)
    ).aio_scalar(as_tuple=True) == (2, 1)

    assert await IntegerTestModel.select(
        fn.MAX(IntegerTestModel.num).alias('max'),
        fn.Min(IntegerTestModel.num).alias('min')
    ).aio_scalar(as_dict=True) == {'max': 2, 'min': 1}

    assert await TestModel.select().aio_scalar() is None


@dbs_all
async def test_count_query(db: AioDatabase) -> None:

    for num in range(5):
        await IntegerTestModel.aio_create(num=num)
    count = await IntegerTestModel.select().limit(3).aio_count()
    assert count == 3


@dbs_all
async def test_count_query_clear_limit(db: AioDatabase) -> None:

    for num in range(5):
        await IntegerTestModel.aio_create(num=num)
    count = await IntegerTestModel.select().limit(3).aio_count(clear_limit=True)
    assert count == 5


@dbs_all
async def test_aio_delete_instance(db: AioDatabase) -> None:
    text = "Test %s" % uuid.uuid4()
    obj1 = await TestModel.aio_create(text=text)
    obj2 = await TestModel.aio_get(id=obj1.id)

    await obj2.aio_delete_instance()

    obj3 = await TestModel.aio_get_or_none(id=obj1.id)
    assert obj3 is None


@dbs_all
async def test_aio_delete_instance_with_fk(db: AioDatabase) -> None:
    alpha = await TestModelAlpha.aio_create(text="test")
    beta = await TestModelBeta.aio_create(alpha=alpha, text="test")

    await alpha.aio_delete_instance(recursive=True)

    assert await TestModelAlpha.aio_get_or_none(id=alpha.id) is None
    assert await TestModelBeta.aio_get_or_none(id=beta.id) is None


@dbs_all
async def test_aio_save(db: AioDatabase) -> None:
    t = TestModel(text="text", data="data")
    rows = await t.aio_save()
    assert rows == 1
    assert t.id is not None

    assert await TestModel.aio_get_or_none(text="text", data="data") is not None


@dbs_all
async def test_aio_save__force_insert(db: AioDatabase) -> None:
    t = await TestModel.aio_create(text="text", data="data")
    t.data = "data2"
    await t.aio_save()

    assert await TestModel.aio_get_or_none(text="text", data="data2") is not None

    with pytest.raises(peewee.IntegrityError):
        await t.aio_save(force_insert=True)


@dbs_all
async def test_aio_get_or_create__get(db: AioDatabase) -> None:
    t1 = await TestModel.aio_create(text="text", data="data")
    t2, created = await TestModel.aio_get_or_create(text="text")
    assert t1.id == t2.id
    assert created is False


@dbs_all
async def test_aio_get_or_create__created(db: AioDatabase) -> None:
    t2, created = await TestModel.aio_get_or_create(text="text")
    assert t2.text == "text"
    assert created is True


@dbs_all
async def test_aio_exists(db: AioDatabase) -> None:
    await TestModel.aio_create(text="text1", data="data")
    await TestModel.aio_create(text="text2", data="data")

    assert await TestModel.select().where(TestModel.data=="data").aio_exists() is True
    assert await TestModel.select().where(TestModel.data == "not_existed").aio_exists() is False


@dbs_all
@pytest.mark.parametrize(
    "prefetch_type",
    peewee.PREFETCH_TYPE.values()
)
async def test_aio_prefetch(db: AioDatabase, prefetch_type: peewee.PREFETCH_TYPE) -> None:
    alpha_1 = await TestModelAlpha.aio_create(text='Alpha 1')
    alpha_2 = await TestModelAlpha.aio_create(text='Alpha 2')

    beta_11 = await TestModelBeta.aio_create(alpha=alpha_1, text='Beta 11')
    beta_12 = await TestModelBeta.aio_create(alpha=alpha_1, text='Beta 12')
    _ = await TestModelBeta.aio_create(
        alpha=alpha_2, text='Beta 21'
    )
    _ = await TestModelBeta.aio_create(
        alpha=alpha_2, text='Beta 22'
    )

    gamma_111 = await TestModelGamma.aio_create(
        beta=beta_11, text='Gamma 111'
    )
    gamma_112 = await TestModelGamma.aio_create(
        beta=beta_11, text='Gamma 112'
    )

    result = await TestModelAlpha.select().order_by(TestModelAlpha.id).aio_prefetch(
        TestModelBeta.select().order_by(TestModelBeta.id),
        TestModelGamma.select().order_by(TestModelGamma.id),
        prefetch_type=prefetch_type,
    )
    assert tuple(result) == (alpha_1, alpha_2)
    assert tuple(result[0].betas) == (beta_11, beta_12)
    assert tuple(result[0].betas[0].gammas) == (gamma_111, gamma_112)
