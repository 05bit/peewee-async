import asyncio
import peewee
import warnings
import logging

from .helpers import _execute_query_async
from .result import AsyncQueryWrapper, AsyncRawQueryWrapper

@asyncio.coroutine
def execute(query):
    """Execute *SELECT*, *INSERT*, *UPDATE* or *DELETE* query asyncronously.

    :param query: peewee query instance created with ``Model.select()``,
                  ``Model.update()`` etc.
    :return: result depends on query type, it's the same as for sync ``query.execute()``
    """
    if isinstance(query, peewee.SelectQuery):
        coroutine = select
    elif isinstance(query, peewee.UpdateQuery):
        coroutine = update
    elif isinstance(query, peewee.InsertQuery):
        coroutine = insert
    elif isinstance(query, peewee.DeleteQuery):
        coroutine = delete
    else:
        coroutine = raw_query

    return (yield from coroutine(query))


@asyncio.coroutine
def create_object(model, **data):
    """Create object asynchronously.
    
    :param model: mode class
    :param data: data for initializing object
    :return: new object saved to database
    """
    # NOTE! Here are internals involved:
    #
    # - obj._data
    # - obj._get_pk_value()
    # - obj._set_pk_value()
    # - obj._prepare_instance()
    #
    warnings.warn("create_object() is deprecated, Manager.create() "
                  "should be used instead",
                  DeprecationWarning)

    obj = model(**data)

    pk = yield from insert(model.insert(**dict(obj._data)))

    if pk is None:
        pk = obj._get_pk_value()
    obj._set_pk_value(pk)
    
    obj._prepare_instance()

    return obj


@asyncio.coroutine
def get_object(source, *args):
    """Get object asynchronously.

    :param source: mode class or query to get object from
    :param args: lookup parameters
    :return: model instance or raises ``peewee.DoesNotExist`` if object not found
    """
    warnings.warn("get_object() is deprecated, Manager.get() "
                  "should be used instead",
                  DeprecationWarning)

    if isinstance(source, peewee.Query):
        query = source
        model = query.model_class
    else:
        query = source.select()
        model = source

    # Return first object from query
    for obj in (yield from select(query.where(*args))):
        return obj

    # No objects found
    raise model.DoesNotExist


@asyncio.coroutine
def delete_object(obj, recursive=False, delete_nullable=False):
    """Delete object asynchronously.

    :param obj: object to delete
    :param recursive: if ``True`` also delete all other objects depends on object
    :param delete_nullable: if `True` and delete is recursive then delete even 'nullable' dependencies

    For details please check out `Model.delete_instance()`_ in peewee docs.

    .. _Model.delete_instance(): http://peewee.readthedocs.io/en/latest/peewee/api.html#Model.delete_instance
    """
    warnings.warn("delete_object() is deprecated, Manager.delete() "
                  "should be used instead",
                  DeprecationWarning)

    # Here are private calls involved:
    # - obj._pk_expr()
    if recursive:
        dependencies = obj.dependencies(delete_nullable)
        for query, fk in reversed(list(dependencies)):
            model = fk.model_class
            if fk.null and not delete_nullable:
                yield from update(model.update(**{fk.name: None}).where(query))
            else:
                yield from delete(model.delete().where(query))
    result = yield from delete(obj.delete().where(obj._pk_expr()))
    return result


@asyncio.coroutine
def update_object(obj, only=None):
    """Update object asynchronously.

    :param obj: object to update
    :param only: list or tuple of fields to updata, is `None` then all fields updated

    This function does the same as `Model.save()`_ for already saved object, but it
    doesn't invoke ``save()`` method on model class. That is important to know if you
    overrided save method for your model.

    .. _Model.save(): http://peewee.readthedocs.io/en/latest/peewee/api.html#Model.save
    """
    # Here are private calls involved:
    #
    # - obj._data
    # - obj._meta
    # - obj._prune_fields()
    # - obj._pk_expr()
    # - obj._dirty.clear()
    #
    warnings.warn("update_object() is deprecated, Manager.update() "
                  "should be used instead",
                  DeprecationWarning)

    field_dict = dict(obj._data)
    pk_field = obj._meta.primary_key

    if only:
        field_dict = obj._prune_fields(field_dict, only)

    if not isinstance(pk_field, peewee.CompositeKey):
        field_dict.pop(pk_field.name, None)
    else:
        field_dict = obj._prune_fields(field_dict, obj.dirty_fields)
    rows = yield from update(obj.update(**field_dict).where(obj._pk_expr()))

    obj._dirty.clear()
    return rows


@asyncio.coroutine
def select(query):
    """Perform SELECT query asynchronously.
    """
    assert isinstance(query, peewee.SelectQuery),\
        ("Error, trying to run select coroutine"
         "with wrong query class %s" % str(query))

    cursor = yield from _execute_query_async(query)

    result = AsyncQueryWrapper(cursor=cursor, query=query)

    try:
        while True:
            yield from result.fetchone()
    except GeneratorExit:
        pass
    finally:
        yield from cursor.release

    yield from cursor.release
    return result


@asyncio.coroutine
def insert(query):
    """Perform INSERT query asynchronously. Returns last insert ID.
    """
    assert isinstance(query, peewee.InsertQuery),\
        ("Error, trying to run insert coroutine"
         "with wrong query class %s" % str(query))

    cursor = yield from _execute_query_async(query)

    try:
        if query.is_insert_returning:
            if query._return_id_list:
                result = map(lambda x: x[0], (yield from cursor.fetchall()))
            else:
                result = (yield from cursor.fetchone())[0]
        else:
            result = yield from query.database.last_insert_id_async(
                cursor, query.model_class)
    finally:
        yield from cursor.release

    return result


@asyncio.coroutine
def update(query):
    """Perform UPDATE query asynchronously. Returns number of rows updated.
    """
    assert isinstance(query, peewee.UpdateQuery),\
        ("Error, trying to run update coroutine"
         "with wrong query class %s" % str(query))

    cursor = yield from _execute_query_async(query)
    rowcount = cursor.rowcount

    yield from cursor.release
    return rowcount


@asyncio.coroutine
def delete(query):
    """Perform DELETE query asynchronously. Returns number of rows deleted.
    """
    assert isinstance(query, peewee.DeleteQuery),\
        ("Error, trying to run delete coroutine"
         "with wrong query class %s" % str(query))

    cursor = yield from _execute_query_async(query)
    rowcount = cursor.rowcount

    yield from cursor.release
    return rowcount


@asyncio.coroutine
def count(query, clear_limit=False):
    """Perform *COUNT* aggregated query asynchronously.

    :return: number of objects in ``select()`` query
    """
    if query._distinct or query._group_by or query._limit or query._offset:
        # wrapped_count()
        clone = query.order_by()
        if clear_limit:
            clone._limit = clone._offset = None

        sql, params = clone.sql()
        wrapped = 'SELECT COUNT(1) FROM (%s) AS wrapped_select' % sql
        raw_query = query.model_class.raw(wrapped, *params)
        return (yield from scalar(raw_query)) or 0
    else:
        # simple count()
        query = query.order_by()
        query._select = [peewee.fn.Count(peewee.SQL('*'))]
        return (yield from scalar(query)) or 0


@asyncio.coroutine
def scalar(query, as_tuple=False):
    """Get single value from ``select()`` query, i.e. for aggregation.

    :return: result is the same as after sync ``query.scalar()`` call
    """
    cursor = yield from _execute_query_async(query)
    row = yield from cursor.fetchone()

    yield from cursor.release
    if row and not as_tuple:
        return row[0]
    else:
        return row


@asyncio.coroutine
def raw_query(query):
    assert isinstance(query, peewee.RawQuery),\
        ("Error, trying to run delete coroutine"
         "with wrong query class %s" % str(query))

    cursor = yield from _execute_query_async(query)

    result = AsyncRawQueryWrapper(cursor=cursor, query=query)
    try:
        while True:
            yield from result.fetchone()
    except GeneratorExit:
        pass
    finally:
        yield from cursor.release

    return result


@asyncio.coroutine
def prefetch(query, *subqueries):
    """Asynchronous version of the `prefetch()` from peewee.

    Returns Query that has already cached data.
    """
    # This code is copied from peewee.prefetch and adopted
    # to use async execute. Also it's a bit hacky, consider
    # it to be experimental!

    if not subqueries:
        return query

    fixed_queries = peewee.prefetch_add_subquery(query, subqueries)

    deps = {}
    rel_map = {}
    for prefetch_result in reversed(fixed_queries):
        query_model = prefetch_result.model
        if prefetch_result.fields:
            for rel_model in prefetch_result.rel_models:
                rel_map.setdefault(rel_model, [])
                rel_map[rel_model].append(prefetch_result)

        deps[query_model] = {}
        id_map = deps[query_model]
        has_relations = bool(rel_map.get(query_model))

        # NOTE! This is hacky, we perform async `execute()` and substitute result
        # to the initial query:

        qr = yield from execute(prefetch_result.query)
        prefetch_result.query._qr = list(qr)
        prefetch_result.query._dirty = False

        for instance in prefetch_result.query._qr:
            if prefetch_result.fields:
                prefetch_result.store_instance(instance, id_map)
            if has_relations:
                for rel in rel_map[query_model]:
                    rel.populate_instance(instance, deps[rel.model])

    return prefetch_result.query
