

def model_has_fields(model, fields):
    for field, value in fields.items():
        if not getattr(model, field) == value:
            return False
    return True
