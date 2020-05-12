import itertools

from dataclasses import dataclass, fields, field, MISSING

from .base import Fragment
from .base import format as Q


def model_field(*, sql, **kwargs):
    return field(**kwargs, metadata={"sql": sql})


class ModelBase:
    @classmethod
    def table_name(cls):
        return Q.identifier(cls.Meta.table_name)

    @classmethod
    def field_names(cls, *, prefix=None, exclude=()):
        return [
            Q.identifier(f.name, prefix=prefix)
            for f in fields(cls)
            if f.name not in exclude
        ]

    def field_values(self, *, exclude=(), default_none=False):
        if default_none:

            def field_value(name):
                value = getattr(self, name)
                return Q.literal("DEFAULT") if value is None else Q("{}", value)

        else:

            def field_value(name):
                return Q("{}", getattr(self, name))

        return [field_value(f.name) for f in fields(self) if f.name not in exclude]

    @classmethod
    def from_tuple(cls, tup, *, offset=0, exclude=()):
        names = (f.name for f in fields(cls) if f.name not in exclude)
        kwargs = {
            name: tup[offset] for offset, name in zip(itertools.count(offset), names)
        }
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, dct, *, exclude=()):
        names = (f.name for f in fields(cls) if f.name not in exclude)
        kwargs = {k: v for k, v in dct.items() if k in names}
        return cls(**kwargs)

    @classmethod
    def create_table_query(cls):
        columns = [
            Q("{} {}", Q.identifier(f.name), Q.literal(f.metadata["sql"]))
            for f in fields(cls)
        ]
        return Q(
            "CREATE TABLE IF NOT EXISTS {name} ({columns})",
            name=cls.table_name(),
            columns=Q.list(columns),
        )

    @classmethod
    def select_query(cls, where=Q.literal("TRUE")):
        return Q(
            "SELECT {fields} FROM {name} WHERE {where}",
            fields=Q.list(cls.field_names()),
            name=cls.table_name(),
            where=where,
        )

    def insert_query(self, exclude=()):
        return Q(
            "INSERT INTO {name} ({fields}) VALUES ({values})",
            name=self.table_name(),
            fields=Q.list(self.field_names(exclude=exclude)),
            values=Q.list(self.field_values(exclude=exclude, default_none=True)),
        )
