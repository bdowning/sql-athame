import datetime
import functools
import uuid
from collections.abc import AsyncGenerator, Iterable, Mapping
from dataclasses import Field, InitVar, dataclass, fields
from typing import (
    Annotated,
    Any,
    Callable,
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
        )


@dataclass
class ConcreteColumnInfo:
    type: str
    create_type: str
    nullable: bool
    constraints: tuple[str, ...]

    @staticmethod
    def from_column_info(name: str, *args: ColumnInfo) -> "ConcreteColumnInfo":
        info = functools.reduce(ColumnInfo.merge, args, ColumnInfo())
        if info.create_type is None and info.type is not None:
            info.create_type = info.type
            info.type = sql_create_type_map.get(info.type.upper(), info.type)
        if type(info.type) is not str or type(info.create_type) is not str:
            raise ValueError(f"Missing SQL type for column {name!r}")
        return ConcreteColumnInfo(
            type=info.type,
            create_type=info.create_type,
            nullable=bool(info.nullable),
            constraints=info._constraints,
        )

    def create_table_string(self) -> str:
        parts = (
            self.create_type,
            *(() if self.nullable else ("NOT NULL",)),
            *self.constraints,
        )
        return " ".join(parts)


NULLABLE_TYPES = (type(None), Any, object)


def split_nullable(typ: type) -> tuple[bool, type]:
    nullable = typ in NULLABLE_TYPES
    if get_origin(typ) is Union:
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


sql_type_map: dict[Any, str] = {
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
    _column_info: Optional[dict[str, ConcreteColumnInfo]]
    _cache: dict[tuple, Any]
    table_name: str
    primary_key_names: tuple[str, ...]
    array_safe_insert: bool
    _type_hints: dict[str, type]

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
        if insert_multiple_mode == "array_safe":
            cls.array_safe_insert = True
        elif insert_multiple_mode == "unnest":
            cls.array_safe_insert = False
        else:
            raise ValueError("Unknown `insert_multiple_mode`")
        if isinstance(primary_key, str):
            cls.primary_key_names = (primary_key,)
        else:
            cls.primary_key_names = tuple(primary_key)

    @classmethod
    def _fields(cls):
        # wrapper to ignore typing weirdness: 'Argument 1 to "fields"
        # has incompatible type "..."; expected "DataclassInstance |
        # type[DataclassInstance]"'
        return fields(cls)  # type: ignore

    @classmethod
    def _cached(cls, key: tuple, thunk: Callable[[], U]) -> U:
        try:
            return cls._cache[key]
        except KeyError:
            cls._cache[key] = thunk()
            return cls._cache[key]

    @classmethod
    def type_hints(cls) -> dict[str, type]:
        try:
            return cls._type_hints
        except AttributeError:
            cls._type_hints = get_type_hints(cls, include_extras=True)
            return cls._type_hints

    @classmethod
    def column_info_for_field(cls, field: Field) -> ConcreteColumnInfo:
        type_info = cls.type_hints()[field.name]
        base_type = type_info
        metadata = []
        if get_origin(type_info) is Annotated:
            base_type, *metadata = get_args(type_info)
        nullable, base_type = split_nullable(base_type)
        info = [ColumnInfo(nullable=nullable)]
        if base_type in sql_type_map:
            info.append(ColumnInfo(type=sql_type_map[base_type]))
        for md in metadata:
            if isinstance(md, ColumnInfo):
                info.append(md)
        return ConcreteColumnInfo.from_column_info(field.name, *info)

    @classmethod
    def column_info(cls, column: str) -> ConcreteColumnInfo:
        try:
            return cls._column_info[column]  # type: ignore
        except AttributeError:
            cls._column_info = {
                f.name: cls.column_info_for_field(f) for f in cls._fields()
            }
            return cls._column_info[column]

    @classmethod
    def table_name_sql(cls, *, prefix: Optional[str] = None) -> Fragment:
        return sql.identifier(cls.table_name, prefix=prefix)

    @classmethod
    def primary_key_names_sql(cls, *, prefix: Optional[str] = None) -> list[Fragment]:
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_key_names]

    @classmethod
    def field_names(cls, *, exclude: FieldNamesSet = ()) -> list[str]:
        return [f.name for f in cls._fields() if f.name not in exclude]

    @classmethod
    def field_names_sql(
        cls, *, prefix: Optional[str] = None, exclude: FieldNamesSet = ()
    ) -> list[Fragment]:
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
        for f in cls._fields():
            if f.name not in exclude:
                func.append(f"self.{f.name},")
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
    def from_tuple(
        cls: type[T], tup: tuple, *, offset: int = 0, exclude: FieldNamesSet = ()
    ) -> T:
        names = (f.name for f in cls._fields() if f.name not in exclude)
        kwargs = {name: tup[offset] for offset, name in enumerate(names, start=offset)}
        return cls(**kwargs)

    @classmethod
    def from_dict(
        cls: type[T], dct: dict[str, Any], *, exclude: FieldNamesSet = ()
    ) -> T:
        names = {f.name for f in cls._fields() if f.name not in exclude}
        kwargs = {k: v for k, v in dct.items() if k in names}
        return cls(**kwargs)

    @classmethod
    def ensure_model(cls: type[T], row: Union[T, Mapping[str, Any]]) -> T:
        if isinstance(row, cls):
            return row
        return cls(**row)

    @classmethod
    def create_table_sql(cls) -> Fragment:
        entries = [
            sql(
                "{} {}",
                sql.identifier(f.name),
                sql.literal(cls.column_info(f.name).create_table_string()),
            )
            for f in cls._fields()
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
    async def select_cursor(
        cls: type[T],
        connection: Connection,
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
        where: Where = (),
        prefetch: int = 1000,
    ) -> AsyncGenerator[T, None]:
        async for row in connection.cursor(
            *cls.select_sql(order_by=order_by, for_update=for_update, where=where),
            prefetch=prefetch,
        ):
            yield cls(**row)

    @classmethod
    async def select(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
        where: Where = (),
    ) -> list[T]:
        return [
            cls(**row)
            for row in await connection_or_pool.fetch(
                *cls.select_sql(order_by=order_by, for_update=for_update, where=where)
            )
        ]

    @classmethod
    def create_sql(cls: type[T], **kwargs: Any) -> Fragment:
        return sql(
            "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING {out_fields}",
            table=cls.table_name_sql(),
            fields=sql.list(sql.identifier(x) for x in kwargs.keys()),
            values=sql.list(sql.value(x) for x in kwargs.values()),
            out_fields=sql.list(cls.field_names_sql()),
        )

    @classmethod
    async def create(
        cls: type[T], connection_or_pool: Union[Connection, Pool], **kwargs: Any
    ) -> T:
        row = await connection_or_pool.fetchrow(*cls.create_sql(**kwargs))
        return cls(**row)

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
        return cached(
            unnest=sql.unnest(
                (row.primary_key() for row in rows),
                (cls.column_info(pk).type for pk in cls.primary_key_names),
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
        return cached(
            unnest=sql.unnest(
                (row.field_values() for row in rows),
                (cls.column_info(name).type for name in cls.field_names()),
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
        if cls.array_safe_insert:
            return await cls.insert_multiple_array_safe(connection_or_pool, rows)
        else:
            return await cls.insert_multiple_unnest(connection_or_pool, rows)

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
        if cls.array_safe_insert:
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
        for f in cls._fields():
            if f.name not in ignore:
                func.append(f" if a.{f.name} != b.{f.name}: return False")
        func += [" return True"]
        exec("\n".join(func), env)
        return env["equal_ignoring"]

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

        if created or updated:
            await cls.upsert_multiple(
                connection, (*created, *updated), insert_only=insert_only
            )
        if deleted:
            await cls.delete_multiple(connection, deleted)

        return created, updated, deleted

    @classmethod
    def _get_differences_ignoring_fn(
        cls: type[T], ignore: FieldNamesSet = ()
    ) -> Callable[[T, T], list[str]]:
        env: dict[str, Any] = {}
        func = [
            "def differences_ignoring(a, b):",
            " diffs = []",
        ]
        for f in cls._fields():
            if f.name not in ignore:
                func.append(f" if a.{f.name} != b.{f.name}: diffs.append({f.name!r})")
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


def chunked(lst, n):
    if type(lst) is not list:
        lst = list(lst)
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
