import datetime
import uuid
from dataclasses import dataclass, field, fields
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from .base import Fragment, sql

WhereType = Union[Fragment, Iterable[Fragment]]


@dataclass
class ColumnInfo:
    type: str
    constraints: List[str]

    def create_table_string(self):
        return " ".join((self.type, *self.constraints))


def model_field(*, type, constraints=(), **kwargs):
    if isinstance(constraints, str):
        constraints = (constraints,)
    info = ColumnInfo(type, constraints)
    return field(**kwargs, metadata={"sql_athame": info})  # type: ignore


sql_type_map = {
    Optional[bool]: ("BOOLEAN",),
    Optional[datetime.date]: ("DATE",),
    Optional[datetime.datetime]: ("TIMESTAMP",),
    Optional[float]: ("DOUBLE PRECISION",),
    Optional[int]: ("INTEGER",),
    Optional[str]: ("TEXT",),
    Optional[uuid.UUID]: ("UUID",),
    bool: ("BOOLEAN", "NOT NULL"),
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
    return ColumnInfo(type, constraints)


T = TypeVar("T", bound="ModelBase")


class ModelBase(Mapping[str, Any]):
    _column_info: Optional[Dict[str, ColumnInfo]]

    def keys(self):
        return self.field_names()

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

    def get(self, key, default):
        return getattr(self, key, default)

    @classmethod
    def column_info(cls) -> Dict[str, ColumnInfo]:
        try:
            return cls._column_info  # type: ignore
        except AttributeError:
            cls._column_info = {f.name: column_info_for_field(f) for f in fields(cls)}
            return cls._column_info

    @classmethod
    def table_name(cls) -> str:
        return cls.Meta.table_name  # type: ignore

    @classmethod
    def primary_key_names(cls) -> Tuple[str]:
        return getattr(cls.Meta, "primary_keys", ())  # type: ignore

    @classmethod
    def table_name_sql(cls, *, prefix=None) -> Fragment:
        return sql.identifier(cls.table_name(), prefix=prefix)

    @classmethod
    def primary_key_names_sql(cls, *, prefix=None) -> List[Fragment]:
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_key_names()]

    @classmethod
    def field_names(cls, *, exclude: Itereable[str] = ()) -> List[str]:
        return [f.name for f in fields(cls) if f.name not in exclude]

    @classmethod
    def field_names_sql(
        cls, *, prefix=None, exclude: Iterable[str] = ()
    ) -> List[Fragment]:
        return [
            sql.identifier(f, prefix=prefix) for f in cls.field_names(exclude=exclude)
        ]

    def primary_key(self) -> tuple:
        return tuple(getattr(self, pk) for pk in self.primary_key_names())

    def field_values(self, *, exclude: Iterable[str] = ()) -> List[Any]:
        return [getattr(self, f.name) for f in fields(self) if f.name not in exclude]

    def field_values_sql(
        self, *, exclude: Iterable[str] = (), default_none=False
    ) -> List[Fragment]:
        if default_none:

            def field_value(name):
                value = getattr(self, name)
                return sql.literal("DEFAULT") if value is None else sql.value(value)

        else:

            def field_value(name):
                return sql.value(getattr(self, name))

        return [field_value(f.name) for f in fields(self) if f.name not in exclude]

    @classmethod
    def from_tuple(
        cls: Type[T], tup: tuple, *, offset=0, exclude: Iterable[str] = ()
    ) -> T:
        names = (f.name for f in fields(cls) if f.name not in exclude)
        kwargs = {name: tup[offset] for offset, name in enumerate(names, start=offset)}
        return cls(**kwargs)  # type: ignore

    @classmethod
    def from_dict(
        cls: Type[T], dct: Dict[str, Any], *, exclude: Iterable[str] = ()
    ) -> T:
        names = {f.name for f in fields(cls) if f.name not in exclude}
        kwargs = {k: v for k, v in dct.items() if k in names}
        return cls(**kwargs)  # type: ignore

    @classmethod
    def ensure_model(cls: Type[T], row: Union[T, Dict[str, Any]]) -> T:
        if isinstance(row, cls):
            return row
        return cls(**row)  # type: ignore

    @classmethod
    def create_table_sql(cls) -> Fragment:
        column_info = cls.column_info()
        entries = [
            sql(
                "{} {}",
                sql.identifier(f.name),
                sql.literal(column_info[f.name].create_table_string()),
            )
            for f in fields(cls)
        ]
        if cls.primary_key_names():
            entries += [sql("PRIMARY KEY ({})", sql.list(cls.primary_key_names_sql()))]
        return sql(
            "CREATE TABLE IF NOT EXISTS {table} ({entries})",
            table=cls.table_name_sql(),
            entries=sql.list(entries),
        )

    @classmethod
    def select_sql(cls, where: WhereType = (), for_update=False) -> Fragment:
        if not isinstance(where, Fragment):
            where = sql.all(where)
        query = sql(
            "SELECT {fields} FROM {name} WHERE {where}",
            fields=sql.list(cls.field_names_sql()),
            name=cls.table_name_sql(),
            where=where,
        )
        if for_update:
            query = sql("{} FOR UPDATE", query)
        return query

    @classmethod
    async def select_cursor(
        cls: Type[T], connection, for_update=False, where: WhereType = ()
    ) -> AsyncGenerator[T, None]:
        async for row in connection.cursor(
            *cls.select_sql(for_update=for_update, where=where)
        ):
            yield cls(**row)  # type: ignore

    @classmethod
    async def select(
        cls: Type[T], connection_or_pool, for_update=False, where: WhereType = ()
    ) -> List[T]:
        return [
            cls(**row)  # type: ignore
            for row in await connection_or_pool.fetch(
                *cls.select_sql(for_update=for_update, where=where)
            )
        ]

    def insert_sql(self, exclude: Iterable[str] = ()) -> Fragment:
        return sql(
            "INSERT INTO {table} ({fields}) VALUES ({values})",
            table=self.table_name_sql(),
            fields=sql.list(self.field_names_sql(exclude=exclude)),
            values=sql.list(self.field_values_sql(exclude=exclude, default_none=True)),
        )

    async def insert(self, connection_or_pool, exclude: Iterable[str] = ()):
        await connection_or_pool.execute(*self.insert_sql(exclude))

    @classmethod
    def upsert_sql(cls, insert_sql: Fragment, exclude: Iterable[str] = ()) -> Fragment:
        return sql(
            "{insert_sql} ON CONFLICT ({pks}) DO UPDATE SET {assignments}",
            insert_sql=insert_sql,
            pks=sql.list(cls.primary_key_names_sql()),
            assignments=sql.list(
                sql("{field}=EXCLUDED.{field}", field=x)
                for x in cls.field_names_sql(
                    exclude=(*cls.primary_key_names(), *exclude)
                )
            ),
        )

    async def upsert(self, connection_or_pool, exclude: Iterable[str] = ()):
        query = sql(
            "{} RETURNING xmax",
            self.upsert_sql(self.insert_sql(exclude=exclude), exclude=exclude),
        )
        result = await connection_or_pool.fetchrow(*query)
        is_update = result["xmax"] != 0
        return is_update

    @classmethod
    def delete_multiple_sql(cls: Type[T], rows: Iterable[T]) -> Fragment:
        column_info = cls.column_info()
        delete = sql(
            "DELETE FROM {table} WHERE ({pks}) IN (SELECT * FROM {unnest})",
            table=cls.table_name_sql(),
            pks=sql.list(sql.identifier(pk) for pk in cls.primary_key_names()),
            unnest=sql.unnest(
                (row.primary_key() for row in rows),
                (column_info[pk].type for pk in cls.primary_key_names()),
            ),
        )
        return delete

    @classmethod
    async def delete_multiple(cls, connection_or_pool, rows):
        await connection_or_pool.execute(*cls.delete_multiple_sql(rows))

    @classmethod
    def insert_multiple_sql(cls: Type[T], rows: Iterable[T]):
        column_info = cls.column_info()
        insert = sql(
            "INSERT INTO {table} ({fields}) SELECT * FROM {unnest}",
            table=cls.table_name_sql(),
            fields=sql.list(cls.field_names_sql()),
            unnest=sql.unnest(
                (row.field_values() for row in rows),
                (column_info[name].type for name in cls.field_names()),
            ),
        )
        return insert

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
        rows: Union[Iterable[T], Iterable[Dict[str, Any]]],
        *,
        where: WhereType,
        ignore: Iterable[str] = ()
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


def equal_ignoring(old, new, ignore):
    keys = (*old.keys(), *new.keys())
    for key in keys:
        if key in ignore:
            continue
        if old.get(key, None) != new.get(key, None):
            return False
    return True
