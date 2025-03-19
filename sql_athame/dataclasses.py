import datetime
import functools
import sys
import uuid
from collections.abc import AsyncGenerator, Iterable, Mapping
from dataclasses import Field, InitVar, dataclass, fields
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from typing_extensions import TypeAlias

from .base import Fragment, sql

Where: TypeAlias = Union[Fragment, Iterable[Fragment]]
# KLUDGE to avoid a string argument being valid
SequenceOfStrings: TypeAlias = Union[list[str], tuple[str, ...]]
FieldNames: TypeAlias = SequenceOfStrings
FieldNamesSet: TypeAlias = Union[SequenceOfStrings, set[str]]

Connection: TypeAlias = Any
Pool: TypeAlias = Any


@dataclass
class ColumnInfo:
    type: Optional[str] = None
    create_type: Optional[str] = None
    nullable: Optional[bool] = None

    _constraints: tuple[str, ...] = ()
    constraints: InitVar[Union[str, Iterable[str], None]] = None

    serialize: Optional[Callable[[Any], Any]] = None
    deserialize: Optional[Callable[[Any], Any]] = None

    def __post_init__(self, constraints: Union[str, Iterable[str], None]) -> None:
        if constraints is not None:
            if type(constraints) is str:
                constraints = (constraints,)
            self._constraints = tuple(constraints)

    @staticmethod
    def merge(a: "ColumnInfo", b: "ColumnInfo") -> "ColumnInfo":
        return ColumnInfo(
            type=b.type if b.type is not None else a.type,
            create_type=b.create_type if b.create_type is not None else a.create_type,
            nullable=b.nullable if b.nullable is not None else a.nullable,
            _constraints=(*a._constraints, *b._constraints),
            serialize=b.serialize if b.serialize is not None else a.serialize,
            deserialize=b.deserialize if b.deserialize is not None else a.deserialize,
        )


@dataclass
class ConcreteColumnInfo:
    field: Field
    type_hint: type
    type: str
    create_type: str
    nullable: bool
    constraints: tuple[str, ...]
    serialize: Optional[Callable[[Any], Any]] = None
    deserialize: Optional[Callable[[Any], Any]] = None

    @staticmethod
    def from_column_info(
        field: Field, type_hint: Any, *args: ColumnInfo
    ) -> "ConcreteColumnInfo":
        info = functools.reduce(ColumnInfo.merge, args, ColumnInfo())
        if info.create_type is None and info.type is not None:
            info.create_type = info.type
            info.type = sql_create_type_map.get(info.type.upper(), info.type)
        if type(info.type) is not str or type(info.create_type) is not str:
            raise ValueError(f"Missing SQL type for column {field.name!r}")
        return ConcreteColumnInfo(
            field=field,
            type_hint=type_hint,
            type=info.type,
            create_type=info.create_type,
            nullable=bool(info.nullable),
            constraints=info._constraints,
            serialize=info.serialize,
            deserialize=info.deserialize,
        )

    def create_table_string(self) -> str:
        parts = (
            self.create_type,
            *(() if self.nullable else ("NOT NULL",)),
            *self.constraints,
        )
        return " ".join(parts)

    def maybe_serialize(self, value: Any) -> Any:
        if self.serialize:
            return self.serialize(value)
        return value


UNION_TYPES: tuple = (Union,)
if sys.version_info >= (3, 10):
    from types import UnionType

    UNION_TYPES = (Union, UnionType)

NULLABLE_TYPES = (type(None), Any, object)


def split_nullable(typ: type) -> tuple[bool, type]:
    nullable = typ in NULLABLE_TYPES
    if get_origin(typ) in UNION_TYPES:
        args = []
        for arg in get_args(typ):
            if arg in NULLABLE_TYPES:
                nullable = True
            else:
                args.append(arg)
        return nullable, Union[tuple(args)]  # type: ignore
    return nullable, typ


sql_create_type_map = {
    "BIGSERIAL": "BIGINT",
    "SERIAL": "INTEGER",
    "SMALLSERIAL": "SMALLINT",
}


sql_type_map: dict[type, str] = {
    bool: "BOOLEAN",
    bytes: "BYTEA",
    datetime.date: "DATE",
    datetime.datetime: "TIMESTAMP",
    float: "DOUBLE PRECISION",
    int: "INTEGER",
    str: "TEXT",
    uuid.UUID: "UUID",
}


T = TypeVar("T", bound="ModelBase")
U = TypeVar("U")


class ModelBase:
    _column_info: dict[str, ConcreteColumnInfo]
    _cache: dict[tuple, Any]
    table_name: str
    primary_key_names: tuple[str, ...]
    insert_multiple_mode: str

    def __init_subclass__(
        cls,
        *,
        table_name: str,
        primary_key: Union[FieldNames, str] = (),
        insert_multiple_mode: str = "unnest",
        **kwargs: Any,
    ):
        cls._cache = {}
        cls.table_name = table_name
        if insert_multiple_mode not in ("array_safe", "unnest", "executemany"):
            raise ValueError("Unknown `insert_multiple_mode`")
        cls.insert_multiple_mode = insert_multiple_mode
        if isinstance(primary_key, str):
            cls.primary_key_names = (primary_key,)
        else:
            cls.primary_key_names = tuple(primary_key)

    @classmethod
    def _cached(cls, key: tuple, thunk: Callable[[], U]) -> U:
        try:
            return cls._cache[key]
        except KeyError:
            cls._cache[key] = thunk()
            return cls._cache[key]

    @classmethod
    def column_info_for_field(cls, field: Field, type_hint: type) -> ConcreteColumnInfo:
        base_type = type_hint
        metadata = []
        if get_origin(type_hint) is Annotated:
            base_type, *metadata = get_args(type_hint)
        nullable, base_type = split_nullable(base_type)
        info = [ColumnInfo(nullable=nullable)]
        if base_type in sql_type_map:
            info.append(ColumnInfo(type=sql_type_map[base_type]))
        for md in metadata:
            if isinstance(md, ColumnInfo):
                info.append(md)
        return ConcreteColumnInfo.from_column_info(field, type_hint, *info)

    @classmethod
    def column_info(cls) -> dict[str, ConcreteColumnInfo]:
        try:
            return cls._column_info
        except AttributeError:
            type_hints = get_type_hints(cls, include_extras=True)
            cls._column_info = {
                f.name: cls.column_info_for_field(f, type_hints[f.name])
                for f in fields(cls)  # type: ignore
            }
            return cls._column_info

    @classmethod
    def table_name_sql(cls, *, prefix: Optional[str] = None) -> Fragment:
        return sql.identifier(cls.table_name, prefix=prefix)

    @classmethod
    def primary_key_names_sql(cls, *, prefix: Optional[str] = None) -> list[Fragment]:
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_key_names]

    @classmethod
    def field_names(cls, *, exclude: FieldNamesSet = ()) -> list[str]:
        return [
            ci.field.name
            for ci in cls.column_info().values()
            if ci.field.name not in exclude
        ]

    @classmethod
    def field_names_sql(
        cls,
        *,
        prefix: Optional[str] = None,
        exclude: FieldNamesSet = (),
        as_prepended: Optional[str] = None,
    ) -> list[Fragment]:
        if as_prepended:
            return [
                sql(
                    "{} AS {}",
                    sql.identifier(f, prefix=prefix),
                    sql.identifier(f"{as_prepended}{f}"),
                )
                for f in cls.field_names(exclude=exclude)
            ]
        return [
            sql.identifier(f, prefix=prefix) for f in cls.field_names(exclude=exclude)
        ]

    def primary_key(self) -> tuple:
        return tuple(getattr(self, pk) for pk in self.primary_key_names)

    @classmethod
    def _get_field_values_fn(
        cls: type[T], exclude: FieldNamesSet = ()
    ) -> Callable[[T], list[Any]]:
        env: dict[str, Any] = {}
        func = ["def get_field_values(self): return ["]
        for ci in cls.column_info().values():
            if ci.field.name not in exclude:
                if ci.serialize:
                    env[f"_ser_{ci.field.name}"] = ci.serialize
                    func.append(f"_ser_{ci.field.name}(self.{ci.field.name}),")
                else:
                    func.append(f"self.{ci.field.name},")
        func += ["]"]
        exec(" ".join(func), env)
        return env["get_field_values"]

    def field_values(self, *, exclude: FieldNamesSet = ()) -> list[Any]:
        get_field_values = self._cached(
            ("get_field_values", tuple(sorted(exclude))),
            lambda: self._get_field_values_fn(exclude),
        )
        return get_field_values(self)

    def field_values_sql(
        self, *, exclude: FieldNamesSet = (), default_none: bool = False
    ) -> list[Fragment]:
        if default_none:
            return [
                sql.literal("DEFAULT") if value is None else sql.value(value)
                for value in self.field_values()
            ]
        else:
            return [sql.value(value) for value in self.field_values()]

    @classmethod
    def _get_from_mapping_fn(cls: type[T]) -> Callable[[Mapping[str, Any]], T]:
        env: dict[str, Any] = {"cls": cls}
        func = ["def from_mapping(mapping):"]
        if not any(ci.deserialize for ci in cls.column_info().values()):
            func.append(" return cls(**mapping)")
        else:
            func.append(" deser_dict = dict(mapping)")
            for ci in cls.column_info().values():
                if ci.deserialize:
                    env[f"_deser_{ci.field.name}"] = ci.deserialize
                    func.append(f" if {ci.field.name!r} in deser_dict:")
                    func.append(
                        f"  deser_dict[{ci.field.name!r}] = _deser_{ci.field.name}(deser_dict[{ci.field.name!r}])"
                    )
            func.append(" return cls(**deser_dict)")
        exec("\n".join(func), env)
        return env["from_mapping"]

    @classmethod
    def from_mapping(cls: type[T], mapping: Mapping[str, Any], /) -> T:
        # KLUDGE nasty but... efficient?
        from_mapping_fn = cls._get_from_mapping_fn()
        cls.from_mapping = from_mapping_fn  # type: ignore
        return from_mapping_fn(mapping)

    @classmethod
    def from_prepended_mapping(
        cls: type[T], mapping: Mapping[str, Any], prepend: str
    ) -> T:
        filtered_dict: dict[str, Any] = {}
        for k, v in mapping.items():
            if k.startswith(prepend):
                filtered_dict[k[len(prepend) :]] = v
        return cls.from_mapping(filtered_dict)

    @classmethod
    def ensure_model(cls: type[T], row: Union[T, Mapping[str, Any]]) -> T:
        if isinstance(row, cls):
            return row
        return cls.from_mapping(row)  # type: ignore

    @classmethod
    def create_table_sql(cls) -> Fragment:
        entries = [
            sql(
                "{} {}",
                sql.identifier(ci.field.name),
                sql.literal(ci.create_table_string()),
            )
            for ci in cls.column_info().values()
        ]
        if cls.primary_key_names:
            entries += [sql("PRIMARY KEY ({})", sql.list(cls.primary_key_names_sql()))]
        return sql(
            "CREATE TABLE IF NOT EXISTS {table} ({entries})",
            table=cls.table_name_sql(),
            entries=sql.list(entries),
        )

    @classmethod
    def select_sql(
        cls,
        where: Where = (),
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
    ) -> Fragment:
        if isinstance(order_by, str):
            order_by = (order_by,)
        if not isinstance(where, Fragment):
            where = sql.all(where)
        cached = cls._cached(
            ("select_sql", tuple(order_by)),
            lambda: sql(
                "SELECT {fields} FROM {name} WHERE {where}{order}",
                fields=sql.list(cls.field_names_sql()),
                name=cls.table_name_sql(),
                order=(
                    sql(" ORDER BY {}", sql.list(sql.identifier(x) for x in order_by))
                    if order_by
                    else sql.literal("")
                ),
            ).compile(),
        )
        query = cached(where=where)
        if for_update:
            query = Fragment([query, " FOR UPDATE"])
        return query

    @classmethod
    async def cursor_from(
        cls: type[T],
        connection: Connection,
        query: Fragment,
        prefetch: int = 1000,
    ) -> AsyncGenerator[T, None]:
        async for row in connection.cursor(*query, prefetch=prefetch):
            yield cls.from_mapping(row)

    @classmethod
    def select_cursor(
        cls: type[T],
        connection: Connection,
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
        where: Where = (),
        prefetch: int = 1000,
    ) -> AsyncGenerator[T, None]:
        return cls.cursor_from(
            connection,
            cls.select_sql(order_by=order_by, for_update=for_update, where=where),
            prefetch=prefetch,
        )

    @classmethod
    async def fetch_from(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        query: Fragment,
    ) -> list[T]:
        return [cls.from_mapping(row) for row in await connection_or_pool.fetch(*query)]

    @classmethod
    async def select(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
        where: Where = (),
    ) -> list[T]:
        return await cls.fetch_from(
            connection_or_pool,
            cls.select_sql(order_by=order_by, for_update=for_update, where=where),
        )

    @classmethod
    def create_sql(cls: type[T], **kwargs: Any) -> Fragment:
        column_info = cls.column_info()
        return sql(
            "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING {out_fields}",
            table=cls.table_name_sql(),
            fields=sql.list(sql.identifier(k) for k in kwargs.keys()),
            values=sql.list(
                sql.value(column_info[k].maybe_serialize(v)) for k, v in kwargs.items()
            ),
            out_fields=sql.list(cls.field_names_sql()),
        )

    @classmethod
    async def create(
        cls: type[T], connection_or_pool: Union[Connection, Pool], **kwargs: Any
    ) -> T:
        row = await connection_or_pool.fetchrow(*cls.create_sql(**kwargs))
        return cls.from_mapping(row)

    def insert_sql(self, exclude: FieldNamesSet = ()) -> Fragment:
        cached = self._cached(
            ("insert_sql", tuple(sorted(exclude))),
            lambda: sql(
                "INSERT INTO {table} ({fields}) VALUES ({values})",
                table=self.table_name_sql(),
                fields=sql.list(self.field_names_sql(exclude=exclude)),
            ).compile(),
        )
        return cached(
            values=sql.list(self.field_values_sql(exclude=exclude, default_none=True)),
        )

    async def insert(
        self, connection_or_pool: Union[Connection, Pool], exclude: FieldNamesSet = ()
    ) -> str:
        return await connection_or_pool.execute(*self.insert_sql(exclude))

    @classmethod
    def upsert_sql(cls, insert_sql: Fragment, exclude: FieldNamesSet = ()) -> Fragment:
        cached = cls._cached(
            ("upsert_sql", tuple(sorted(exclude))),
            lambda: sql(
                " ON CONFLICT ({pks}) DO UPDATE SET {assignments}",
                insert_sql=insert_sql,
                pks=sql.list(cls.primary_key_names_sql()),
                assignments=sql.list(
                    sql("{field}=EXCLUDED.{field}", field=x)
                    for x in cls.field_names_sql(
                        exclude=(*cls.primary_key_names, *exclude)
                    )
                ),
            ).flatten(),
        )
        return Fragment([insert_sql, cached])

    async def upsert(
        self, connection_or_pool: Union[Connection, Pool], exclude: FieldNamesSet = ()
    ) -> bool:
        query = sql(
            "{} RETURNING xmax",
            self.upsert_sql(self.insert_sql(exclude=exclude), exclude=exclude),
        )
        result = await connection_or_pool.fetchrow(*query)
        return result["xmax"] != 0

    @classmethod
    def delete_multiple_sql(cls: type[T], rows: Iterable[T]) -> Fragment:
        cached = cls._cached(
            ("delete_multiple_sql",),
            lambda: sql(
                "DELETE FROM {table} WHERE ({pks}) IN (SELECT * FROM {unnest})",
                table=cls.table_name_sql(),
                pks=sql.list(sql.identifier(pk) for pk in cls.primary_key_names),
            ).compile(),
        )
        column_info = cls.column_info()
        return cached(
            unnest=sql.unnest(
                (row.primary_key() for row in rows),
                (column_info[pk].type for pk in cls.primary_key_names),
            ),
        )

    @classmethod
    async def delete_multiple(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        return await connection_or_pool.execute(*cls.delete_multiple_sql(rows))

    @classmethod
    def insert_multiple_sql(cls: type[T], rows: Iterable[T]) -> Fragment:
        cached = cls._cached(
            ("insert_multiple_sql",),
            lambda: sql(
                "INSERT INTO {table} ({fields}) SELECT * FROM {unnest}",
                table=cls.table_name_sql(),
                fields=sql.list(cls.field_names_sql()),
            ).compile(),
        )
        column_info = cls.column_info()
        return cached(
            unnest=sql.unnest(
                (row.field_values() for row in rows),
                (column_info[name].type for name in cls.field_names()),
            ),
        )

    @classmethod
    def insert_multiple_array_safe_sql(cls: type[T], rows: Iterable[T]) -> Fragment:
        return sql(
            "INSERT INTO {table} ({fields}) VALUES {values}",
            table=cls.table_name_sql(),
            fields=sql.list(cls.field_names_sql()),
            values=sql.list(
                sql("({})", sql.list(row.field_values_sql(default_none=True)))
                for row in rows
            ),
        )

    @classmethod
    def insert_multiple_executemany_chunk_sql(
        cls: type[T], chunk_size: int
    ) -> Fragment:
        def generate() -> Fragment:
            columns = len(cls.column_info())
            values = ", ".join(
                f"({', '.join(f'${i}' for i in chunk)})"
                for chunk in chunked(range(1, columns * chunk_size + 1), columns)
            )
            return sql(
                "INSERT INTO {table} ({fields}) VALUES {values}",
                table=cls.table_name_sql(),
                fields=sql.list(cls.field_names_sql()),
                values=sql.literal(values),
            ).flatten()

        return cls._cached(
            ("insert_multiple_executemany_chunk", chunk_size),
            generate,
        )

    @classmethod
    async def insert_multiple_executemany(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> None:
        args = [r.field_values() for r in rows]
        query = cls.insert_multiple_executemany_chunk_sql(1).query()[0]
        if args:
            await connection_or_pool.executemany(query, args)

    @classmethod
    async def insert_multiple_unnest(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        return await connection_or_pool.execute(*cls.insert_multiple_sql(rows))

    @classmethod
    async def insert_multiple_array_safe(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        last = ""
        for chunk in chunked(rows, 100):
            last = await connection_or_pool.execute(
                *cls.insert_multiple_array_safe_sql(chunk)
            )
        return last

    @classmethod
    async def insert_multiple(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        if cls.insert_multiple_mode == "executemany":
            await cls.insert_multiple_executemany(connection_or_pool, rows)
            return "INSERT"
        elif cls.insert_multiple_mode == "array_safe":
            return await cls.insert_multiple_array_safe(connection_or_pool, rows)
        else:
            return await cls.insert_multiple_unnest(connection_or_pool, rows)

    @classmethod
    async def upsert_multiple_executemany(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
    ) -> None:
        args = [r.field_values() for r in rows]
        query = cls.upsert_sql(
            cls.insert_multiple_executemany_chunk_sql(1), exclude=insert_only
        ).query()[0]
        if args:
            await connection_or_pool.executemany(query, args)

    @classmethod
    async def upsert_multiple_unnest(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
    ) -> str:
        return await connection_or_pool.execute(
            *cls.upsert_sql(cls.insert_multiple_sql(rows), exclude=insert_only)
        )

    @classmethod
    async def upsert_multiple_array_safe(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
    ) -> str:
        last = ""
        for chunk in chunked(rows, 100):
            last = await connection_or_pool.execute(
                *cls.upsert_sql(
                    cls.insert_multiple_array_safe_sql(chunk), exclude=insert_only
                )
            )
        return last

    @classmethod
    async def upsert_multiple(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
    ) -> str:
        if cls.insert_multiple_mode == "executemany":
            await cls.upsert_multiple_executemany(
                connection_or_pool, rows, insert_only=insert_only
            )
            return "INSERT"
        elif cls.insert_multiple_mode == "array_safe":
            return await cls.upsert_multiple_array_safe(
                connection_or_pool, rows, insert_only=insert_only
            )
        else:
            return await cls.upsert_multiple_unnest(
                connection_or_pool, rows, insert_only=insert_only
            )

    @classmethod
    def _get_equal_ignoring_fn(
        cls: type[T], ignore: FieldNamesSet = ()
    ) -> Callable[[T, T], bool]:
        env: dict[str, Any] = {}
        func = ["def equal_ignoring(a, b):"]
        for ci in cls.column_info().values():
            if ci.field.name not in ignore:
                func.append(f" if a.{ci.field.name} != b.{ci.field.name}: return False")
        func += [" return True"]
        exec("\n".join(func), env)
        return env["equal_ignoring"]

    @classmethod
    async def plan_replace_multiple(
        cls: type[T],
        connection: Connection,
        rows: Union[Iterable[T], Iterable[Mapping[str, Any]]],
        *,
        where: Where,
        ignore: FieldNamesSet = (),
        insert_only: FieldNamesSet = (),
    ) -> "ReplaceMultiplePlan[T]":
        ignore = sorted(set(ignore) | set(insert_only))
        equal_ignoring = cls._cached(
            ("equal_ignoring", tuple(ignore)),
            lambda: cls._get_equal_ignoring_fn(ignore),
        )
        pending = {row.primary_key(): row for row in map(cls.ensure_model, rows)}

        updated = []
        deleted = []

        async for old in cls.select_cursor(
            connection, where=where, order_by=cls.primary_key_names, for_update=True
        ):
            pk = old.primary_key()
            if pk not in pending:
                deleted.append(old)
            else:
                if not equal_ignoring(old, pending[pk]):
                    updated.append(pending[pk])
                del pending[pk]

        created = list(pending.values())

        return ReplaceMultiplePlan(cls, insert_only, created, updated, deleted)

    @classmethod
    async def replace_multiple(
        cls: type[T],
        connection: Connection,
        rows: Union[Iterable[T], Iterable[Mapping[str, Any]]],
        *,
        where: Where,
        ignore: FieldNamesSet = (),
        insert_only: FieldNamesSet = (),
    ) -> tuple[list[T], list[T], list[T]]:
        plan = await cls.plan_replace_multiple(
            connection, rows, where=where, ignore=ignore, insert_only=insert_only
        )
        await plan.execute(connection)
        return plan.cud

    @classmethod
    def _get_differences_ignoring_fn(
        cls: type[T], ignore: FieldNamesSet = ()
    ) -> Callable[[T, T], list[str]]:
        env: dict[str, Any] = {}
        func = [
            "def differences_ignoring(a, b):",
            " diffs = []",
        ]
        for ci in cls.column_info().values():
            if ci.field.name not in ignore:
                func.append(
                    f" if a.{ci.field.name} != b.{ci.field.name}: diffs.append({ci.field.name!r})"
                )
        func += [" return diffs"]
        exec("\n".join(func), env)
        return env["differences_ignoring"]

    @classmethod
    async def replace_multiple_reporting_differences(
        cls: type[T],
        connection: Connection,
        rows: Union[Iterable[T], Iterable[Mapping[str, Any]]],
        *,
        where: Where,
        ignore: FieldNamesSet = (),
        insert_only: FieldNamesSet = (),
    ) -> tuple[list[T], list[tuple[T, T, list[str]]], list[T]]:
        ignore = sorted(set(ignore) | set(insert_only))
        differences_ignoring = cls._cached(
            ("differences_ignoring", tuple(ignore)),
            lambda: cls._get_differences_ignoring_fn(ignore),
        )

        pending = {row.primary_key(): row for row in map(cls.ensure_model, rows)}

        updated_triples = []
        deleted = []

        async for old in cls.select_cursor(
            connection, where=where, order_by=cls.primary_key_names, for_update=True
        ):
            pk = old.primary_key()
            if pk not in pending:
                deleted.append(old)
            else:
                diffs = differences_ignoring(old, pending[pk])
                if diffs:
                    updated_triples.append((old, pending[pk], diffs))
                del pending[pk]

        created = list(pending.values())

        if created or updated_triples:
            await cls.upsert_multiple(
                connection,
                (*created, *(t[1] for t in updated_triples)),
                insert_only=insert_only,
            )
        if deleted:
            await cls.delete_multiple(connection, deleted)

        return created, updated_triples, deleted


@dataclass
class ReplaceMultiplePlan(Generic[T]):
    model_class: type[T]
    insert_only: FieldNamesSet
    created: list[T]
    updated: list[T]
    deleted: list[T]

    @property
    def cud(self) -> tuple[list[T], list[T], list[T]]:
        return (self.created, self.updated, self.deleted)

    async def execute_upserts(self, connection: Connection) -> None:
        if self.created or self.updated:
            await self.model_class.upsert_multiple(
                connection, (*self.created, *self.updated), insert_only=self.insert_only
            )

    async def execute_deletes(self, connection: Connection) -> None:
        if self.deleted:
            await self.model_class.delete_multiple(connection, self.deleted)

    async def execute(self, connection: Connection) -> None:
        await self.execute_upserts(connection)
        await self.execute_deletes(connection)


def chunked(lst, n):
    if type(lst) is not list:
        lst = list(lst)
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
