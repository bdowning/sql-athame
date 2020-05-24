import datetime
import uuid
from dataclasses import dataclass, field, fields
from typing import List, Optional

from .base import Fragment, sql


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
    return field(**kwargs, metadata={"sql_athame": info})


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


class ModelBase:
    def keys(self):
        return self.field_names()

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default):
        return getattr(self, key, default)

    @classmethod
    def column_info(cls):
        try:
            return cls._column_info
        except AttributeError:
            cls._column_info = {f.name: column_info_for_field(f) for f in fields(cls)}
            return cls._column_info

    @classmethod
    def table_name(cls):
        return cls.Meta.table_name

    @classmethod
    def primary_keys(cls):
        return getattr(cls.Meta, "primary_keys", ())

    @classmethod
    def table_name_sql(cls, *, prefix=None):
        return sql.identifier(cls.table_name(), prefix=prefix)

    @classmethod
    def primary_keys_sql(cls, *, prefix=None):
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_keys()]

    @classmethod
    def field_names(cls, *, exclude=()):
        return [f.name for f in fields(cls) if f.name not in exclude]

    @classmethod
    def field_names_sql(cls, *, prefix=None, exclude=()):
        return [
            sql.identifier(f, prefix=prefix) for f in cls.field_names(exclude=exclude)
        ]

    def field_values_sql(self, *, exclude=(), default_none=False):
        if default_none:

            def field_value(name):
                value = getattr(self, name)
                return sql.literal("DEFAULT") if value is None else sql.value(value)

        else:

            def field_value(name):
                return sql.value(getattr(self, name))

        return [field_value(f.name) for f in fields(self) if f.name not in exclude]

    @classmethod
    def from_tuple(cls, tup, *, offset=0, exclude=()):
        names = (f.name for f in fields(cls) if f.name not in exclude)
        kwargs = {name: tup[offset] for offset, name in enumerate(names, start=offset)}
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, dct, *, exclude=()):
        names = {f.name for f in fields(cls) if f.name not in exclude}
        kwargs = {k: v for k, v in dct.items() if k in names}
        return cls(**kwargs)

    @classmethod
    def create_table_sql(cls):
        column_info = cls.column_info()
        entries = [
            sql(
                "{} {}",
                sql.identifier(f.name),
                sql.literal(column_info[f.name].create_table_string()),
            )
            for f in fields(cls)
        ]
        if cls.primary_keys():
            entries += [sql("PRIMARY KEY ({})", sql.list(cls.primary_keys_sql()))]
        return sql(
            "CREATE TABLE IF NOT EXISTS {table} ({entries})",
            table=cls.table_name_sql(),
            entries=sql.list(entries),
        )

    @classmethod
    def select_sql(cls, where=(), for_update=False):
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
    async def select_cursor(cls, connection, for_update=False, where=()):
        async for row in connection.cursor(
            *cls.select_sql(for_update=for_update, where=where)
        ):
            yield cls(*row)

    @classmethod
    async def select(cls, connection_or_pool, for_update=False, where=()):
        return [
            cls(*row)
            for row in await connection_or_pool.fetch(
                *cls.select_sql(for_update=for_update, where=where)
            )
        ]

    def insert_sql(self, exclude=()):
        return sql(
            "INSERT INTO {table} ({fields}) VALUES ({values})",
            table=self.table_name_sql(),
            fields=sql.list(self.field_names_sql(exclude=exclude)),
            values=sql.list(self.field_values_sql(exclude=exclude, default_none=True)),
        )

    async def insert(self, connection_or_pool, exclude=()):
        await connection_or_pool.fetchrow(*self.insert_sql(exclude))

    def upsert_sql(self, exclude=()):
        return sql(
            "{insert_sql} ON CONFLICT ({pks}) DO UPDATE SET {assignments}",
            insert_sql=self.insert_sql(exclude=exclude),
            pks=sql.list(self.primary_keys_sql()),
            assignments=sql.list(
                sql("{field}=EXCLUDED.{field}", field=x)
                for x in self.field_names_sql(exclude=(*self.primary_keys(), *exclude))
            ),
        )

    async def upsert(self, connection_or_pool, exclude=()):
        query = sql("{} RETURNING xmax", self.upsert_sql(exclude=exclude))
        result = await connection_or_pool.fetchrow(*query)
        is_update = result["xmax"] != 0
        return is_update
