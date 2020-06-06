import datetime
import uuid
from dataclasses import dataclass, field, fields
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from .base import Fragment, sql

Where = Union[Fragment, Iterable[Fragment]]
# KLUDGE to avoid a string argument being valid
SequenceOfStrings = Union[List[str], Tuple[str, ...]]
FieldNames = SequenceOfStrings
FieldNamesSet = Union[SequenceOfStrings, Set[str]]


@dataclass
class ColumnInfo:
    type: str
    create_type: str
    constraints: Tuple[str, ...]

    def create_table_string(self):
        return " ".join((self.create_type, *self.constraints))


def model_field_metadata(
    type: str, constraints: Union[str, Iterable[str]] = ()
) -> Dict[str, Any]:
    if isinstance(constraints, str):
        constraints = (constraints,)
    info = ColumnInfo(
        sql_create_type_map.get(type.upper(), type), type, tuple(constraints)
    )
    return {"sql_athame": info}


def model_field(*, type: str, constraints: Union[str, Iterable[str]] = (), **kwargs):
    return field(**kwargs, metadata=model_field_metadata(type, constraints))  # type: ignore


sql_create_type_map = {
    "BIGSERIAL": "BIGINT",
    "SERIAL": "INTEGER",
    "SMALLSERIAL": "SMALLINT",
}


sql_type_map = {
    Optional[bool]: ("BOOLEAN",),
    Optional[bytes]: ("BYTEA",),
    Optional[datetime.date]: ("DATE",),
    Optional[datetime.datetime]: ("TIMESTAMP",),
    Optional[float]: ("DOUBLE PRECISION",),
    Optional[int]: ("INTEGER",),
    Optional[str]: ("TEXT",),
    Optional[uuid.UUID]: ("UUID",),
    bool: ("BOOLEAN", "NOT NULL"),
    bytes: ("BYTEA", "NOT NULL"),
    datetime.date: ("DATE", "NOT NULL"),
    datetime.datetime: ("TIMESTAMP", "NOT NULL"),
    float: ("DOUBLE PRECISION", "NOT NULL"),
    int: ("INTEGER", "NOT NULL"),
    str: ("TEXT", "NOT NULL"),
    uuid.UUID: ("UUID", "NOT NULL"),
}


def column_info_for_field(field):
    if "sql_athame" in field.metadata:
        return field.metadata["sql_athame"]
    type, *constraints = sql_type_map[field.type]
    return ColumnInfo(type, type, tuple(constraints))


T = TypeVar("T", bound="ModelBase")
U = TypeVar("U")


class ModelBase(Mapping[str, Any]):
    _column_info: Optional[Dict[str, ColumnInfo]]
    _cache: Dict[tuple, Any]
    table_name: str
    primary_key_names: Tuple[str, ...]

    def __init_subclass__(
        cls, *, table_name: str, primary_key: Union[FieldNames, str] = (), **kwargs
    ):
        cls._cache = {}
        cls.table_name = table_name
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

    def keys(self):
        return self.field_names()

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

    def get(self, key, default=None):
        return getattr(self, key, default)

    @classmethod
    def column_info(cls, column: str) -> ColumnInfo:
        try:
            return cls._column_info[column]  # type: ignore
        except AttributeError:
            cls._column_info = {f.name: column_info_for_field(f) for f in fields(cls)}
            return cls._column_info[column]

    @classmethod
    def table_name_sql(cls, *, prefix=None) -> Fragment:
        return sql.identifier(cls.table_name, prefix=prefix)

    @classmethod
    def primary_key_names_sql(cls, *, prefix=None) -> List[Fragment]:
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_key_names]

    @classmethod
    def field_names(cls, *, exclude: FieldNamesSet = ()) -> List[str]:
        return [f.name for f in fields(cls) if f.name not in exclude]

    @classmethod
    def field_names_sql(
        cls, *, prefix=None, exclude: FieldNamesSet = ()
    ) -> List[Fragment]:
        return [
            sql.identifier(f, prefix=prefix) for f in cls.field_names(exclude=exclude)
        ]

    def primary_key(self) -> tuple:
        return tuple(getattr(self, pk) for pk in self.primary_key_names)

    @classmethod
    def _get_field_values_fn(cls, exclude: FieldNamesSet = ()):
        env: Dict[str, Any] = dict()
        func = ["def get_field_values(self): return ["]
        for f in fields(cls):
            if f.name not in exclude:
                func.append(f"self.{f.name},")
        func += ["]"]
        exec(" ".join(func), env)
        return env["get_field_values"]

    def field_values(self, *, exclude: FieldNamesSet = ()) -> List[Any]:
        get_field_values = self._cached(
            ("get_field_values", tuple(sorted(exclude))),
            lambda: self._get_field_values_fn(exclude),
        )
        return get_field_values(self)

    def field_values_sql(
        self, *, exclude: FieldNamesSet = (), default_none=False
    ) -> List[Fragment]:
        if default_none:
            return [
                sql.literal("DEFAULT") if value is None else sql.value(value)
                for value in self.field_values()
            ]
        else:
            return [sql.value(value) for value in self.field_values()]

    @classmethod
    def from_tuple(
        cls: Type[T], tup: tuple, *, offset=0, exclude: FieldNamesSet = ()
    ) -> T:
        names = (f.name for f in fields(cls) if f.name not in exclude)
        kwargs = {name: tup[offset] for offset, name in enumerate(names, start=offset)}
        return cls(**kwargs)  # type: ignore

    @classmethod
    def from_dict(
        cls: Type[T], dct: Dict[str, Any], *, exclude: FieldNamesSet = ()
    ) -> T:
        names = {f.name for f in fields(cls) if f.name not in exclude}
        kwargs = {k: v for k, v in dct.items() if k in names}
        return cls(**kwargs)  # type: ignore

    @classmethod
    def ensure_model(cls: Type[T], row: Union[T, Mapping[str, Any]]) -> T:
        if isinstance(row, cls):
            return row
        return cls(**row)  # type: ignore

    @classmethod
    def create_table_sql(cls) -> Fragment:
        entries = [
            sql(
                "{} {}",
                sql.identifier(f.name),
                sql.literal(cls.column_info(f.name).create_table_string()),
            )
            for f in fields(cls)
        ]
        if cls.primary_key_names:
            entries += [sql("PRIMARY KEY ({})", sql.list(cls.primary_key_names_sql()))]
        return sql(
            "CREATE TABLE IF NOT EXISTS {table} ({entries})",
            table=cls.table_name_sql(),
            entries=sql.list(entries),
        )

    @classmethod
    def select_sql(cls, where: Where = (), for_update=False) -> Fragment:
        if not isinstance(where, Fragment):
            where = sql.all(where)
        cached = cls._cached(
            ("select_sql",),
            lambda: sql(
                "SELECT {fields} FROM {name} WHERE {where}",
                fields=sql.list(cls.field_names_sql()),
                name=cls.table_name_sql(),
            ).compile(),
        )
        query = cached(where=where)
        if for_update:
            query = Fragment([query, " FOR UPDATE"], {})
        return query

    @classmethod
    async def select_cursor(
        cls: Type[T], connection, for_update=False, where: Where = ()
    ) -> AsyncGenerator[T, None]:
        async for row in connection.cursor(
            *cls.select_sql(for_update=for_update, where=where)
        ):
            yield cls(**row)  # type: ignore

    @classmethod
    async def select(
        cls: Type[T], connection_or_pool, for_update=False, where: Where = ()
    ) -> List[T]:
        return [
            cls(**row)  # type: ignore
            for row in await connection_or_pool.fetch(
                *cls.select_sql(for_update=for_update, where=where)
            )
        ]

    @classmethod
    def create_sql(cls: Type[T], **kwargs) -> Fragment:
        return sql(
            "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING {out_fields}",
            table=cls.table_name_sql(),
            fields=sql.list(sql.identifier(x) for x in kwargs.keys()),
            values=sql.list(sql.value(x) for x in kwargs.values()),
            out_fields=sql.list(cls.field_names_sql()),
        )

    @classmethod
    async def create(cls: Type[T], connection_or_pool, **kwargs) -> T:
        row = await connection_or_pool.fetchrow(*cls.create_sql(**kwargs))
        return cls(**row)  # type: ignore

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

    async def insert(self, connection_or_pool, exclude: FieldNamesSet = ()):
        await connection_or_pool.execute(*self.insert_sql(exclude))

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
        return Fragment([insert_sql, cached], {})

    async def upsert(self, connection_or_pool, exclude: FieldNamesSet = ()) -> bool:
        query = sql(
            "{} RETURNING xmax",
            self.upsert_sql(self.insert_sql(exclude=exclude), exclude=exclude),
        )
        result = await connection_or_pool.fetchrow(*query)
        is_update = result["xmax"] != 0
        return is_update

    @classmethod
    def delete_multiple_sql(cls: Type[T], rows: Iterable[T]) -> Fragment:
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
    async def delete_multiple(cls: Type[T], connection_or_pool, rows: Iterable[T]):
        await connection_or_pool.execute(*cls.delete_multiple_sql(rows))

    @classmethod
    def insert_multiple_sql(cls: Type[T], rows: Iterable[T]) -> Fragment:
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
    async def insert_multiple(cls: Type[T], connection_or_pool, rows: Iterable[T]):
        await connection_or_pool.execute(*cls.insert_multiple_sql(rows))

    @classmethod
    async def upsert_multiple(cls: Type[T], connection_or_pool, rows: Iterable[T]):
        await connection_or_pool.execute(*cls.upsert_sql(cls.insert_multiple_sql(rows)))

    @classmethod
    async def replace_multiple(
        cls: Type[T],
        connection,
        rows: Union[Iterable[T], Iterable[Mapping[str, Any]]],
        *,
        where: Where,
        ignore: FieldNamesSet = (),
    ) -> Tuple[List[T], List[T], List[T]]:
        pending = {row.primary_key(): row for row in map(cls.ensure_model, rows)}

        updated = []
        deleted = []

        async for old in cls.select_cursor(connection, where=where, for_update=True):
            pk = old.primary_key()
            if pk not in pending:
                deleted.append(old)
            else:
                if not equal_ignoring(old, pending[pk], ignore):
                    updated.append(pending[pk])
                del pending[pk]

        created = list(pending.values())

        if created or updated:
            await cls.upsert_multiple(connection, (*created, *updated))
        if deleted:
            await cls.delete_multiple(connection, deleted)

        return created, updated, deleted


def equal_ignoring(
    old: Mapping[str, Any], new: Mapping[str, Any], ignore: FieldNamesSet
) -> bool:
    keys = (*old.keys(), *new.keys())
    for key in keys:
        if key in ignore:
            continue
        if old.get(key, None) != new.get(key, None):
            return False
    return True
