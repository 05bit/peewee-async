import asyncio

from tests.conftest import manager_for_all_dbs
from tests.models import TestModel


class FakeUpdateError(Exception):
    """Fake error while updating database.
    """
    pass


@manager_for_all_dbs
async def test_atomic_success(manager):
    obj = await manager.create(TestModel, text='FOO')
    obj_id = obj.id

    async with manager.atomic():
        obj.text = 'BAR'
        await manager.update(obj)

    res = await manager.get(TestModel, id=obj_id)
    assert res.text == 'BAR'


@manager_for_all_dbs
async def test_atomic_failed(manager):
    """Failed update in transaction.
    """

    obj = await manager.create(TestModel, text='FOO')
    obj_id = obj.id

    try:
        async with manager.atomic():
            obj.text = 'BAR'
            await manager.update(obj)
            raise FakeUpdateError()
    except FakeUpdateError as e:
        error = True
        res = await manager.get(TestModel, id=obj_id)

    assert error is True
    assert res.text == 'FOO'


@manager_for_all_dbs
async def test_acid_when_connetion_has_been_brooken(manager):
    # TODO REWRITE FOR NEW STYLE TRANSACTIONS
    async def restart_connections(event_for_lock: asyncio.Event) -> None:
        event_for_lock.set()
        # С этого момента БД доступна, пулл в порядке, всё хорошо. Таски могут работать работу
        await asyncio.sleep(0.05)

        # Ниже происходит падение метеорита
        # (приложением обнаруживается разрыв коннекта с БД и оно сливает пулл + заполняет заново)
        # (может выглядеть нереалистично, но такое бывает и на проде, мы просто симулируем это редкое событие)
        #
        # Мы через event запрещаем таскам трогать БД на время переподнятия пулла.
        # Это нужно, чтобы воспроизвести очень редкие условия, при которых peewee-async косячит.
        event_for_lock.clear()

        await manager.database.close_async()
        await manager.database.connect_async()

        event_for_lock.set()

        # БД самопочинилась (пулл заполнен и готов к работе).
        # С этого момента таски опять могут работать работу
        return None

    async def insert_records(event_for_wait: asyncio.Event):
        await event_for_wait.wait()
        async with manager.transaction():
            # BEGIN
            # INSERT 1
            await manager.create(TestModel, text="1")

            # Это место для падения метеорита.
            # Тут произойдёт разрыв соединения и подключение заново.
            # event здесь нужен чтобы таска не упала с исключением, а воспроизвела редкое поведение peewee-async
            await asyncio.sleep(0.05)
            await event_for_wait.wait()
            # # Метеорит позади. event-loop вернул управление в таску
            #
            # # INSERT 2
            await manager.create(TestModel, text="2")
            # END ?

        return None


    # Этот event-семафор нужно чтобы удобно воссоздать редкую ситуацию,
    # когда разрыв + восстановление происходит до того, как в asyncio.Task вернётся управление.
    # В дикой природе такое происходит редко и при определённых стечениях обстоятельств,
    # но приносит ощутимый ущерб
    event = asyncio.Event()

    results = await asyncio.gather(
        restart_connections(event),
        insert_records(event),
        return_exceptions=True,
    )
    # Проверяем, что ни одна из тасок не упала с исключением
    # assert results == [None, None]

    # (!) Убеждаемся, что атомарность работает (!)
    # Т.е. у нас должны либо закоммититься 2 записи, либо ни одной
    a = list(await manager.execute(TestModel.select()))
    assert len(a) == 0, f'WTF, peewee-async ?! Saved rows: {a}'
    # Если assert выше упал, то в БД оказалась 1 запись, а не 0 или 2.
    # Хотя мы на уровне кода пытались гарантировать, что такого не будет
