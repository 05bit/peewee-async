from typing import Any, Literal, cast

import peewee
from peewee import PREFETCH_TYPE
from typing_extensions import Self

from .databases import AioDatabase
from .result_wrappers import fetch_models
from .utils import CursorProtocol


class AioSchemaManager(peewee.SchemaManager):
    async def aio_create_table(self, safe: bool = True, **options: Any) -> None:
        await self.database.aio_execute(self._create_table(safe=safe, **options))

    async def aio_drop_table(self, safe: bool = True, **options: Any) -> None:
        await self.database.aio_execute(self._drop_table(safe=safe, **options))

    async def aio_truncate_table(self, restart_identity: bool = False, cascade: bool = False) -> None:
        await self.database.aio_execute(self._truncate_table(restart_identity, cascade))

    async def aio_create_indexes(self, safe: bool = True) -> None:
        for query in self._create_indexes(safe=safe):
            await self.database.aio_execute(query)

    async def aio_drop_indexes(self, safe: bool = True) -> None:
        for query in self._drop_indexes(safe=safe):
            await self.database.aio_execute(query)

    async def _aio_create_sequence(self, field: peewee.Field) -> Any:
        self._check_sequences(field)
        if not await self.database.aio_sequence_exists(field.sequence):
            return (self
                    ._create_context()
                    .literal('CREATE SEQUENCE ')
                    .sql(self._sequence_for_field(field)))

    async def aio_create_sequence(self, field: peewee.Field) -> None:
        seq_ctx = await self._aio_create_sequence(field)
        if seq_ctx is not None:
            await self.database.aio_execute(seq_ctx)

    async def aio_create_sequences(self) -> None:
        if self.database.sequences:
            for field in self.model._meta.sorted_fields:
                if field.sequence:
                    await self.aio_create_sequence(field)

    async def aio_create_all(
            self, 
            safe: bool = True, 
            **table_options: Any
        ) -> None:
        await self.aio_create_sequences()
        await self.aio_create_table(safe, **table_options)
        await self.aio_create_indexes(safe=safe)


async def aio_prefetch(sq: Any, *subqueries: Any, prefetch_type: PREFETCH_TYPE = PREFETCH_TYPE.WHERE) -> Any:
    """Asynchronous version of `prefetch()`.

    See also:
    http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#prefetch
    """
    if not subqueries:
        return sq

    fixed_queries = peewee.prefetch_add_subquery(sq, subqueries, prefetch_type)
    deps: dict[Any, Any] = {}
    rel_map: dict[Any, Any] = {}

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

    async def fetch_results(self, database: AioDatabase, cursor: CursorProtocol) -> Any:
        return await fetch_models(cursor, self)


class _AioWriteQueryMixin(AioQueryMixin):
    async def fetch_results(self, database: AioDatabase, cursor: CursorProtocol) -> Any:
        if self._return_cursor:  # type: ignore
            return await fetch_models(cursor, self)
        return await database.aio_rows_affected(cursor)


class AioModelDelete(peewee.ModelDelete, _AioWriteQueryMixin): ...


class AioModelUpdate(peewee.ModelUpdate, _AioWriteQueryMixin): ...


class AioModelInsert(peewee.ModelInsert, AioQueryMixin):
    async def fetch_results(self, database: AioDatabase, cursor: CursorProtocol) -> list[Any] | Any | int:
        if self._returning is None and database.returning_clause and self.table._primary_key:  # type: ignore
            self._returning = (self.table._primary_key,)
            return await database.aio_last_insert_id(cursor, self)
        if self._return_cursor:
            return await fetch_models(cursor, self)
        if self._as_rowcount:
            return await database.aio_rows_affected(cursor)
        return await database.aio_last_insert_id(cursor, self)


class AioModelRaw(peewee.ModelRaw, AioQueryMixin):
    pass


class AioSelectMixin(AioQueryMixin, peewee.SelectBase):
    @peewee.database_required
    async def aio_peek(self, database: AioDatabase, n: int = 1) -> Any:
        """
        Asynchronous version of
        `peewee.SelectBase.peek <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.peek>`_
        """

        async def fetch_results(database: AioDatabase, cursor: CursorProtocol) -> Any:
            return await fetch_models(cursor, self, n)

        rows = await database.aio_execute(self, fetch_results=fetch_results)
        if rows:
            return rows[0] if n == 1 else rows

    @peewee.database_required
    async def aio_scalar(self, database: AioDatabase, as_tuple: bool = False, as_dict: bool = False) -> Any:
        """
        Asynchronous version of `peewee.SelectBase.scalar
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.scalar>`_
        """
        if as_dict:
            return await self.dicts().aio_peek(database)
        row = await self.tuples().aio_peek(database)

        return row[0] if row and not as_tuple else row

    @peewee.database_required
    async def aio_first(self, database: AioDatabase, n: int = 1) -> Any:
        """
        Asynchronous version of `peewee.SelectBase.first
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.first>`_
        """

        if self._limit != n:  # type: ignore
            self._limit = n
        return await self.aio_peek(database, n=n)

    async def aio_get(self, database: AioDatabase | None = None) -> Any:
        """
        Asynchronous version of `peewee.SelectBase.get
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.get>`_
        """
        clone = self.paginate(1, 1)
        try:
            return (await clone.aio_execute(database))[0]
        except IndexError:
            sql, params = clone.sql()
            raise self.model.DoesNotExist(  # noqa: B904
                f"{clone.model} instance matching query does not exist:\nSQL: {sql}\nParams: {params}"
            )

    @peewee.database_required
    async def aio_count(self, database: AioDatabase, clear_limit: bool = False) -> int:
        """
        Asynchronous version of `peewee.SelectBase.count
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.count>`_
        """
        clone = self.order_by().alias("_wrapped")
        if clear_limit:
            clone._limit = clone._offset = None
        try:
            if (
                clone._having is None
                and clone._group_by is None
                and clone._windows is None
                and clone._distinct is None
                and clone._simple_distinct is not True
            ):
                clone = clone.select(peewee.SQL("1"))
        except AttributeError:
            pass
        return cast("int", await AioSelect([clone], [peewee.fn.COUNT(peewee.SQL("1"))]).aio_scalar(database))

    @peewee.database_required
    async def aio_exists(self, database: AioDatabase) -> bool:
        """
        Asynchronous version of `peewee.SelectBase.exists
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#SelectBase.exists>`_
        """
        clone = self.columns(peewee.SQL("1"))
        clone._limit = 1
        clone._offset = None
        return bool(await clone.aio_scalar())

    def union_all(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, "UNION ALL", rhs)

    __add__ = union_all

    def union(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, "UNION", rhs)

    __or__ = union

    def intersect(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, "INTERSECT", rhs)

    __and__ = intersect

    def except_(self, rhs: Any) -> "AioModelCompoundSelectQuery":
        return AioModelCompoundSelectQuery(self.model, self, "EXCEPT", rhs)

    __sub__ = except_

    def aio_prefetch(self, *subqueries: Any, prefetch_type: PREFETCH_TYPE = PREFETCH_TYPE.WHERE) -> Any:
        """
        Asynchronous version of `peewee.ModelSelect.prefetch
        <https://docs.peewee-orm.com/en/latest/peewee/api.html#ModelSelect.prefetch>`_
        """
        return aio_prefetch(self, *subqueries, prefetch_type=prefetch_type)


class AioSelect(AioSelectMixin, peewee.Select):
    pass


class AioModelSelect(AioSelectMixin, peewee.ModelSelect):
    """Asynchronous version of **peewee.ModelSelect** that provides async versions of ModelSelect methods"""

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

    class Meta:
        schema_manager_class = AioSchemaManager

    @classmethod
    async def aio_table_exists(cls) -> bool:
        M = cls._meta
        return cast(
            "bool",
            await cls._schema.database.aio_table_exists(
                M.table.__name__, M.schema
                )
        )

    @classmethod
    async def aio_create_table(cls, safe: bool = True, **options: Any) -> None:

        if safe and not cls._schema.database.safe_create_index \
           and await cls.aio_table_exists():
            return
        if cls._meta.temporary:
            options.setdefault('temporary', cls._meta.temporary)
        await cls._schema.aio_create_all(safe, **options)

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
        columns = [getattr(cls, field) if isinstance(field, str) else field for field in fields]
        return AioModelInsert(cls, insert=query, columns=columns)

    @classmethod
    def raw(cls, sql: str | None, *params: list[Any] | None) -> AioModelRaw:
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
        return cast("int", await type(self).delete().where(self._pk_expr()).aio_execute())

    async def aio_save(self, force_insert: bool = False, only: Any = None) -> int | Literal[False]:  # noqa: C901
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
                raise ValueError("no data to save!")
            rows = await self.update(**field_dict).where(self._pk_expr()).aio_execute()
        elif pk_field is not None:
            pk = await self.insert(**field_dict).aio_execute()
            if pk is not None and (self._meta.auto_increment or pk_value is None):
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
        return cast("Self", await sq.aio_get())

    @classmethod
    async def aio_get_or_none(cls, *query: Any, **filters: Any) -> Self | None:
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
    async def aio_get_or_create(cls, **kwargs: Any) -> tuple[Self, bool]:
        """
        Async version of **peewee.Model.get_or_create**

        See also:
        http://docs.peewee-orm.com/en/3.15.3/peewee/api.html#Model.get_or_create
        """
        defaults = kwargs.pop("defaults", {})
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
                    raise exc  # noqa: B904
