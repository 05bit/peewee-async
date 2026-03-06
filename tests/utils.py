from typing import Any

from peewee_async import AioModel


def model_has_fields(model: AioModel, fields: dict[str, Any]) -> bool:
    for field, value in fields.items():
        if not getattr(model, field) == value:
            return False
    return True
