from typing import Dict, Any

from peewee_async import AioModel


def model_has_fields(model: AioModel, fields: Dict[str, Any]) -> bool:
    for field, value in fields.items():
        if not getattr(model, field) == value:
            return False
    return True
