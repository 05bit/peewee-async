from tests.conftest import manager_for_all_dbs
from tests.models import CompositeTestModel


@manager_for_all_dbs
async def test_composite_key(manager):
    task_id = 5
    product_type = "boots"
    comp = await manager.create(CompositeTestModel, task_id=task_id, product_type=product_type)
    assert comp.get_id() == (task_id, product_type)
