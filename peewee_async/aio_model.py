import peewee
from peewee import PREFETCH_TYPE

from .databases import AioDatabase
from .result_wrappers import fetch_models
from .utils import CursorProtocol
from typing_extensions import Self
from typing import Tuple, List, Any, cast, Optional, Dict, Union


async def aio_prefetch(sq: Any, *subqueries: Any, prefetch_type: PREFETCH_TYPE = PREFETCH_TYPE.WHERE) -> Any:
    """Asynchronous version of `prefetch()`.

    See also:
    http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#prefetch
    """
    if not subqueries:
        return sq

    fixed_queries = peewee.prefetch_add_subquery(sq, subqueries, prefetch_type)
    deps: Dict[Any, Any] = {}
    rel_map: Dict[Any, Any] = {}

    for pq in reversed(fixed_queries):
        query_model = pq.model
        if pq.fields:
            for rel_model in pq.rel_models:
                rel_map.setdefault(rel_model, [])
                rel_map[rel_model].append(pq)

        deps[query_model] = {}
        id_map = deps[query_model]
        has_relations = bool(rel_map.get(query_model))

        result = await pq.query.aio_execute()

        for instance in result:
            if pq.fields:
                pq.store_instance(instance, id_map)
            if has_relations:
                for rel in rel_map[query_model]:
                    rel.populate_instance(instance, deps[rel.model])

    return result


class AioQueryMixin:
    @peewee.database_required
    async def aio_execute(self, database: AioDatabase) -> Any:
        return await database.aio_execute(self)

    async def fetch_results(self, cursor: CursorProtocol) -> Any:
        return await fetch_models(cursor, self)


class AioModelDelete(peewee.ModelDelete, AioQueryMixin):
    async def fetch_results(self, cursor: CursorProtocol) -> Union[List[Any], int]:
        if self._returning:
            return await fetch_models(cursor, self)
        return cursor.rowcount


class AioModelUpdate(peewee.ModelUpdate, AioQueryMixin):

    async def fetch_results(self, cursor: CursorProtocol) -> Union[List[Any], int]:
        if self._returning:
            return await fetch_models(cursor, self)
        return cursor.rowcount


class AioModelInsert(peewee.ModelInsert, AioQueryMixin):
    async def fetch_results(self, cursor: CursorProtocol) -> Union[List[Any], Any, int]:
        if self._returning is not None and len(self._returning) > 1:
            return await fetch_models(cursor, self)

        if self._returning:
            row = await cursor.fetchone()
            return row[0] if row else None
        else:
            return cursor.lastrowid


class AioModelRaw(peewee.ModelRaw, AioQueryMixin):
    pass


class AioSelectMixin(AioQueryMixin):

    @peewee.database_required
    async def aio_scalar(self, database: AioDatabase, as_tuple: bool = False) -> Any:
        """
        Get single value from ``select()`` query, i.e. for aggregation.

        :return: result is the same as after sync ``query.scalar()`` call

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#SelectBase.scalar
        """
        async def fetch_results(cursor: CursorProtocol) -> Any:
            return await cursor.fetchone()

        rows = await database.aio_execute(self, fetch_results=fetch_results)

        return rows[0] if rows and not as_tuple else rows

    async def aio_get(self, database: Optional[AioDatabase] = None) -> Any:
        """
        Async version of **peewee.SelectBase.get**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#SelectBase.get
        """
        clone = self.paginate(1, 1) # type: ignore
        try:
            return (await clone.aio_execute(database))[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist('%s instance matching query does '  # type: ignore
                                          'not exist:\nSQL: %s\nParams: %s' %
                                          (clone.model, sql, params))

    @peewee.database_required
    async def aio_count(self, database: AioDatabase, clear_limit: bool = False) -> int:
        """
        Async version of **peewee.SelectBase.count**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#SelectBase.count
        """
        clone = self.order_by().alias('_wrapped')  # type: ignore
        if clear_limit:
            clone._limit = clone._offset = None
        try:
            if clone._having is None and clone._group_by is None and \
               clone._windows is None and clone._distinct is None and \
               clone._simple_distinct is not True:
                clone = clone.select(peewee.SQL('1'))
        except AttributeError:
            pass
        return cast(
            int,
            await AioSelect([clone], [peewee.fn.COUNT(peewee.SQL('1'))]).aio_scalar(database)
        )

    @peewee.database_required
    async def aio_exists(self, database: AioDatabase) -> bool:
        """
        Async version of **peewee.SelectBase.exists**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#SelectBase.exists
        """
        clone = self.columns(peewee.SQL('1'))  # type: ignore
        clone._limit = 1
        clone._offset = None
        return bool(await clone.aio_scalar())

    def union_all(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, 'UNION ALL', rhs)  # type: ignore
    __add__ = union_all

    def union(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, 'UNION', rhs)  # type: ignore
    __or__ = union

    def intersect(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, 'INTERSECT', rhs)  # type: ignore
    __and__ = intersect

    def except_(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, 'EXCEPT', rhs)  # type: ignore
    __sub__ = except_

    def aio_prefetch(self, *subqueries: Any, prefetch_type: PREFETCH_TYPE = PREFETCH_TYPE.WHERE) -> Any:
        """
        Async version of **peewee.ModelSelect.prefetch**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#ModelSelect.prefetch
        """
        return aio_prefetch(self, *subqueries, prefetch_type=prefetch_type)


class AioSelect(AioSelectMixin, peewee.Select):
    pass


class AioModelSelect(AioSelectMixin, peewee.ModelSelect):
    """Async version of **peewee.ModelSelect** that provides async versions of ModelSelect methods
    """
    pass


class AioModelCompoundSelectQuery(AioSelectMixin, peewee.ModelCompoundSelectQuery):
    pass


class AioModel(peewee.Model):
    """Async version of **peewee.Model** that allows to execute queries asynchronously
    with **aio_execute** method

    Example::

        class User(peewee_async.AioModel):
            username = peewee.CharField(max_length=40, unique=True)

        await User.select().where(User.username == 'admin').aio_execute()

    Also it provides async versions of **peewee.Model** shortcuts

    Example::

        user = await User.aio_get(User.username == 'user')
    """

    @classmethod
    def select(cls, *fields: Any) -> AioModelSelect:
        is_default = not fields
        if not fields:
            fields = cls._meta.sorted_fields
        return AioModelSelect(cls, fields, is_default=is_default)

    @classmethod
    def update(cls, __data: Any = None, **update: Any) -> AioModelUpdate:
        return AioModelUpdate(cls, cls._normalize_data(__data, update))

    @classmethod
    def insert(cls, __data: Any = None, **insert: Any) -> AioModelInsert:
        return AioModelInsert(cls, cls._normalize_data(__data, insert))

    @classmethod
    def insert_many(cls, rows: Any, fields: Any = None) -> AioModelInsert:
        return AioModelInsert(cls, insert=rows, columns=fields)

    @classmethod
    def insert_from(cls, query: Any, fields: Any) -> AioModelInsert:
        columns = [getattr(cls, field) if isinstance(field, str)
                   else field for field in fields]
        return AioModelInsert(cls, insert=query, columns=columns)

    @classmethod
    def raw(cls, sql: Optional[str], *params: Optional[List[Any]]) -> AioModelRaw:
        return AioModelRaw(cls, sql, params)

    @classmethod
    def delete(cls) -> AioModelDelete:
        return AioModelDelete(cls)

    async def aio_delete_instance(self, recursive: bool = False, delete_nullable: bool = False) -> int:
        """
        Async version of **peewee.Model.delete_instance**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.delete_instance
        """
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                model = fk.model
                if fk.null and not delete_nullable:
                    await model.update(**{fk.name: None}).where(query).aio_execute()
                else:
                    await model.delete().where(query).aio_execute()
        return cast(int, await type(self).delete().where(self._pk_expr()).aio_execute())

    async def aio_save(self, force_insert: bool = False, only: Any =None) -> int:
        """
        Async version of **peewee.Model.save**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.save
        """
        field_dict = self.__data__.copy()
        if self._meta.primary_key is not False:
            pk_field = self._meta.primary_key
            pk_value = self._pk  # type: ignore
        else:
            pk_field = pk_value = None
        if only is not None:
            field_dict = self._prune_fields(field_dict, only)
        elif self._meta.only_save_dirty and not force_insert:
            field_dict = self._prune_fields(field_dict, self.dirty_fields)
            if not field_dict:
                self._dirty.clear()
                return False

        self._populate_unsaved_relations(field_dict)
        rows = 1

        if self._meta.auto_increment and pk_value is None:
            field_dict.pop(pk_field.name, None)

        if pk_value is not None and not force_insert:
            if self._meta.composite_key:
                for pk_part_name in pk_field.field_names:
                    field_dict.pop(pk_part_name, None)
            else:
                field_dict.pop(pk_field.name, None)
            if not field_dict:
                raise ValueError('no data to save!')
            rows = await self.update(**field_dict).where(self._pk_expr()).aio_execute()
        elif pk_field is not None:
            pk = await self.insert(**field_dict).aio_execute()
            if pk is not None and (self._meta.auto_increment or
                                   pk_value is None):
                self._pk = pk
                # Although we set the primary-key, do not mark it as dirty.
                self._dirty.discard(pk_field.name)
        else:
            await self.insert(**field_dict).aio_execute()

        self._dirty -= set(field_dict)  # Remove any fields we saved.
        return rows

    @classmethod
    async def aio_get(cls, *query: Any, **filters: Any) -> Self:
        """Async version of **peewee.Model.get**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.get
        """
        sq = cls.select()
        if query:
            if len(query) == 1 and isinstance(query[0], int):
                sq = sq.where(cls._meta.primary_key == query[0])
            else:
                sq = sq.where(*query)
        if filters:
            sq = sq.filter(**filters)
        return cast(Self, await sq.aio_get())

    @classmethod
    async def aio_get_or_none(cls, *query: Any, **filters: Any) -> Optional[Self]:
        """
        Async version of **peewee.Model.get_or_none**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.get_or_none
        """
        try:
            return await cls.aio_get(*query, **filters)
        except cls.DoesNotExist:
            return None

    @classmethod
    async def aio_create(cls, **query: Any) -> Self:
        """
        Async version of **peewee.Model.create**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.create
        """
        inst = cls(**query)
        await inst.aio_save(force_insert=True)
        return inst

    @classmethod
    async def aio_get_or_create(cls, **kwargs: Any) -> Tuple[Self, bool]:
        """
        Async version of **peewee.Model.get_or_create**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.get_or_create
        """
        defaults = kwargs.pop('defaults', {})
        query = cls.select()
        for field, value in kwargs.items():
            query = query.where(getattr(cls, field) == value)

        try:
            return await query.aio_get(), False
        except cls.DoesNotExist:
            try:
                if defaults:
                    kwargs.update(defaults)
                async with cls._meta.database.aio_atomic():
                    return await cls.aio_create(**kwargs), True
            except peewee.IntegrityError as exc:
                try:
                    return await query.aio_get(), False
                except cls.DoesNotExist:
                    raise exc
