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
    """Column metadata for dataclass fields.

    This class specifies SQL column properties that can be applied to dataclass fields
    to control how they are mapped to database columns.

    Attributes:
        type: SQL type name for query parameters (e.g., 'TEXT', 'INTEGER')
        create_type: SQL type for CREATE TABLE statements (defaults to type if not specified)
        nullable: Whether the column allows NULL values (inferred from Optional types if not specified)
        constraints: Additional SQL constraints (e.g., 'UNIQUE', 'CHECK (value > 0)')
        serialize: Function to transform Python values before database storage
        deserialize: Function to transform database values back to Python objects
        insert_only: Whether this field should only be set on INSERT, not UPDATE in upsert operations

    Example:
        >>> from dataclasses import dataclass
        >>> from typing import Annotated
        >>> from sql_athame import ModelBase, ColumnInfo
        >>> import json
        >>>
        >>> @dataclass
        ... class Product(ModelBase, table_name="products", primary_key="id"):
        ...     id: int
        ...     name: str
        ...     price: Annotated[float, ColumnInfo(constraints="CHECK (price > 0)")]
        ...     tags: Annotated[list, ColumnInfo(type="JSONB", serialize=json.dumps, deserialize=json.loads)]
        ...     created_at: Annotated[datetime, ColumnInfo(insert_only=True)]
    """

    type: Optional[str] = None
    create_type: Optional[str] = None
    nullable: Optional[bool] = None

    _constraints: tuple[str, ...] = ()
    constraints: InitVar[Union[str, Iterable[str], None]] = None

    serialize: Optional[Callable[[Any], Any]] = None
    deserialize: Optional[Callable[[Any], Any]] = None
    insert_only: Optional[bool] = None

    def __post_init__(self, constraints: Union[str, Iterable[str], None]) -> None:
        if constraints is not None:
            if type(constraints) is str:
                constraints = (constraints,)
            self._constraints = tuple(constraints)

    @staticmethod
    def merge(a: "ColumnInfo", b: "ColumnInfo") -> "ColumnInfo":
        """Merge two ColumnInfo instances, with b taking precedence over a.

        Args:
            a: Base ColumnInfo
            b: ColumnInfo to overlay on top of a

        Returns:
            New ColumnInfo with b's non-None values overriding a's values
        """
        return ColumnInfo(
            type=b.type if b.type is not None else a.type,
            create_type=b.create_type if b.create_type is not None else a.create_type,
            nullable=b.nullable if b.nullable is not None else a.nullable,
            _constraints=(*a._constraints, *b._constraints),
            serialize=b.serialize if b.serialize is not None else a.serialize,
            deserialize=b.deserialize if b.deserialize is not None else a.deserialize,
            insert_only=b.insert_only if b.insert_only is not None else a.insert_only,
        )


@dataclass
class ConcreteColumnInfo:
    """Resolved column information for a specific dataclass field.

    This is the final, computed column metadata after resolving type hints,
    merging ColumnInfo instances, and applying defaults.

    Attributes:
        field: The dataclass Field object
        type_hint: The resolved Python type hint
        type: SQL type for query parameters
        create_type: SQL type for CREATE TABLE statements
        nullable: Whether the column allows NULL values
        constraints: Tuple of SQL constraint strings
        serialize: Optional serialization function
        deserialize: Optional deserialization function
        insert_only: Whether this field should only be set on INSERT, not UPDATE
    """

    field: Field
    type_hint: type
    type: str
    create_type: str
    nullable: bool
    constraints: tuple[str, ...]
    serialize: Optional[Callable[[Any], Any]] = None
    deserialize: Optional[Callable[[Any], Any]] = None
    insert_only: bool = False

    @staticmethod
    def from_column_info(
        field: Field, type_hint: Any, *args: ColumnInfo
    ) -> "ConcreteColumnInfo":
        """Create ConcreteColumnInfo from a field and its ColumnInfo metadata.

        Args:
            field: The dataclass Field
            type_hint: The resolved type hint for the field
            *args: ColumnInfo instances to merge (later ones take precedence)

        Returns:
            ConcreteColumnInfo with all metadata resolved

        Raises:
            ValueError: If no SQL type can be determined for the field
        """
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
            insert_only=bool(info.insert_only),
        )

    def create_table_string(self) -> str:
        """Generate the SQL column definition for CREATE TABLE statements.

        Returns:
            SQL string like "TEXT NOT NULL CHECK (length > 0)"
        """
        parts = (
            self.create_type,
            *(() if self.nullable else ("NOT NULL",)),
            *self.constraints,
        )
        return " ".join(parts)

    def maybe_serialize(self, value: Any) -> Any:
        """Apply serialization function if configured, otherwise return value unchanged.

        Args:
            value: The Python value to potentially serialize

        Returns:
            Serialized value if serialize function is configured, otherwise original value
        """
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
        """Cache computation results by key.

        Args:
            key: Cache key tuple
            thunk: Function to compute the value if not cached

        Returns:
            Cached or computed value
        """
        try:
            return cls._cache[key]
        except KeyError:
            cls._cache[key] = thunk()
            return cls._cache[key]

    @classmethod
    def column_info_for_field(cls, field: Field, type_hint: type) -> ConcreteColumnInfo:
        """Generate ConcreteColumnInfo for a dataclass field.

        Analyzes the field's type hint and metadata to determine SQL column properties.
        Looks for ColumnInfo in the field's metadata and merges it with type-based defaults.

        Args:
            field: The dataclass Field object
            type_hint: The resolved type hint for the field

        Returns:
            ConcreteColumnInfo with all column metadata resolved
        """
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
        """Get column information for all fields in this model.

        Returns a cached mapping of field names to their resolved column information.
        This is computed once per class and cached for performance.

        Returns:
            Dictionary mapping field names to ConcreteColumnInfo objects
        """
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
        """Generate SQL fragment for the table name.

        Args:
            prefix: Optional schema or alias prefix

        Returns:
            Fragment containing the properly quoted table identifier

        Example:
            >>> list(User.table_name_sql())
            ['"users"']
            >>> list(User.table_name_sql(prefix="public"))
            ['"public"."users"']
        """
        return sql.identifier(cls.table_name, prefix=prefix)

    @classmethod
    def primary_key_names_sql(cls, *, prefix: Optional[str] = None) -> list[Fragment]:
        """Generate SQL fragments for primary key column names.

        Args:
            prefix: Optional table alias prefix

        Returns:
            List of Fragment objects for each primary key column
        """
        return [sql.identifier(pk, prefix=prefix) for pk in cls.primary_key_names]

    @classmethod
    def field_names(cls, *, exclude: FieldNamesSet = ()) -> list[str]:
        """Get list of field names for this model.

        Args:
            exclude: Field names to exclude from the result

        Returns:
            List of field names as strings
        """
        return [
            ci.field.name
            for ci in cls.column_info().values()
            if ci.field.name not in exclude
        ]

    @classmethod
    def insert_only_field_names(cls) -> set[str]:
        """Get set of field names marked as insert_only in ColumnInfo.

        Returns:
            Set of field names that should only be set on INSERT, not UPDATE
        """
        return cls._cached(
            ("insert_only_field_names",),
            lambda: {
                ci.field.name for ci in cls.column_info().values() if ci.insert_only
            },
        )

    @classmethod
    def field_names_sql(
        cls,
        *,
        prefix: Optional[str] = None,
        exclude: FieldNamesSet = (),
        as_prepended: Optional[str] = None,
    ) -> list[Fragment]:
        """Generate SQL fragments for field names.

        Args:
            prefix: Optional table alias prefix for column names
            exclude: Field names to exclude from the result
            as_prepended: If provided, generate "column AS prepended_column" aliases

        Returns:
            List of Fragment objects for each field

        Example:
            >>> list(sql.list(User.field_names_sql()))
            ['"id", "name", "email"']
            >>> list(sql.list(User.field_names_sql(prefix="u")))
            ['"u"."id", "u"."name", "u"."email"']
            >>> list(sql.list(User.field_names_sql(as_prepended="user_")))
            ['"id" AS "user_id", "name" AS "user_name", "email" AS "user_email"']
        """
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
        """Get the primary key value(s) for this instance.

        Returns:
            Tuple containing the primary key field values

        Example:
            >>> user = User(id=UUID(...), name="Alice")
            >>> user.primary_key()
            (UUID('...'),)
        """
        return tuple(getattr(self, pk) for pk in self.primary_key_names)

    @classmethod
    def _get_field_values_fn(
        cls: type[T], exclude: FieldNamesSet = ()
    ) -> Callable[[T], list[Any]]:
        """Generate optimized function to extract field values from instances.

        This method generates and compiles a function that efficiently extracts
        field values from model instances, applying serialization where needed.

        Args:
            exclude: Field names to exclude from value extraction

        Returns:
            Compiled function that takes an instance and returns field values
        """
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
        """Get field values for this instance, with serialization applied.

        Args:
            exclude: Field names to exclude from the result

        Returns:
            List of field values in the same order as field_names()

        Note:
            This method applies any configured serialize functions to the values.
        """
        get_field_values = self._cached(
            ("get_field_values", tuple(sorted(exclude))),
            lambda: self._get_field_values_fn(exclude),
        )
        return get_field_values(self)

    def field_values_sql(
        self, *, exclude: FieldNamesSet = (), default_none: bool = False
    ) -> list[Fragment]:
        """Generate SQL fragments for field values.

        Args:
            exclude: Field names to exclude
            default_none: If True, None values become DEFAULT literals instead of NULL

        Returns:
            List of Fragment objects containing value placeholders or DEFAULT
        """
        if default_none:
            return [
                sql.literal("DEFAULT") if value is None else sql.value(value)
                for value in self.field_values()
            ]
        else:
            return [sql.value(value) for value in self.field_values()]

    @classmethod
    def _get_from_mapping_fn(cls: type[T]) -> Callable[[Mapping[str, Any]], T]:
        """Generate optimized function to create instances from mappings.

        This method generates and compiles a function that efficiently creates
        model instances from dictionary-like mappings, applying deserialization
        where needed.

        Returns:
            Compiled function that takes a mapping and returns a model instance
        """
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
        """Create a model instance from a dictionary-like mapping.

        This method applies any configured deserialize functions to the values
        before creating the instance.

        Args:
            mapping: Dictionary-like object with field names as keys

        Returns:
            New instance of this model class

        Example:
            >>> row = {"id": UUID(...), "name": "Alice", "email": None}
            >>> user = User.from_mapping(row)
        """
        # KLUDGE nasty but... efficient?
        from_mapping_fn = cls._get_from_mapping_fn()
        cls.from_mapping = from_mapping_fn  # type: ignore
        return from_mapping_fn(mapping)

    @classmethod
    def from_prepended_mapping(
        cls: type[T], mapping: Mapping[str, Any], prepend: str
    ) -> T:
        """Create a model instance from a mapping with prefixed keys.

        Useful for creating instances from JOIN query results where columns
        are prefixed to avoid name conflicts.

        Args:
            mapping: Dictionary with prefixed keys
            prepend: Prefix to strip from keys

        Returns:
            New instance of this model class

        Example:
            >>> row = {"user_id": UUID(...), "user_name": "Alice", "user_email": None}
            >>> user = User.from_prepended_mapping(row, "user_")
        """
        filtered_dict: dict[str, Any] = {}
        for k, v in mapping.items():
            if k.startswith(prepend):
                filtered_dict[k[len(prepend) :]] = v
        return cls.from_mapping(filtered_dict)

    @classmethod
    def ensure_model(cls: type[T], row: Union[T, Mapping[str, Any]]) -> T:
        """Ensure the input is a model instance, converting from mapping if needed.

        Args:
            row: Either a model instance or a mapping to convert

        Returns:
            Model instance
        """
        if isinstance(row, cls):
            return row
        return cls.from_mapping(row)  # type: ignore

    @classmethod
    def create_table_sql(cls) -> Fragment:
        """Generate CREATE TABLE SQL for this model.

        Returns:
            Fragment containing CREATE TABLE IF NOT EXISTS statement

        Example:
            >>> list(User.create_table_sql())
            ['CREATE TABLE IF NOT EXISTS "users" ("id" UUID NOT NULL, "name" TEXT NOT NULL, "email" TEXT, PRIMARY KEY ("id"))']
        """
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
        """Generate SELECT SQL for this model.

        Args:
            where: WHERE conditions as Fragment or iterable of Fragments
            order_by: ORDER BY field names
            for_update: Whether to add FOR UPDATE clause

        Returns:
            Fragment containing SELECT statement

        Example:
            >>> list(User.select_sql(where=sql("name = {}", "Alice")))
            ['SELECT "id", "name", "email" FROM "users" WHERE name = $1', 'Alice']
        """
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
        """Create an async generator from a query result.

        Args:
            connection: Database connection
            query: SQL query Fragment
            prefetch: Number of rows to prefetch

        Yields:
            Model instances from the query results
        """
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
        """Create an async generator for SELECT results.

        Args:
            connection: Database connection
            order_by: ORDER BY field names
            for_update: Whether to add FOR UPDATE clause
            where: WHERE conditions
            prefetch: Number of rows to prefetch

        Yields:
            Model instances from the SELECT results

        Example:
            >>> async for user in User.select_cursor(conn, where=sql("active = {}", True)):
            ...     print(user.name)
        """
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
        """Execute a query and return model instances.

        Args:
            connection_or_pool: Database connection or pool
            query: SQL query Fragment

        Returns:
            List of model instances from the query results
        """
        return [cls.from_mapping(row) for row in await connection_or_pool.fetch(*query)]

    @classmethod
    async def select(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        order_by: Union[FieldNames, str] = (),
        for_update: bool = False,
        where: Where = (),
    ) -> list[T]:
        """Execute a SELECT query and return model instances.

        Args:
            connection_or_pool: Database connection or pool
            order_by: ORDER BY field names
            for_update: Whether to add FOR UPDATE clause
            where: WHERE conditions

        Returns:
            List of model instances from the SELECT results

        Example:
            >>> users = await User.select(pool, where=sql("active = {}", True))
        """
        return await cls.fetch_from(
            connection_or_pool,
            cls.select_sql(order_by=order_by, for_update=for_update, where=where),
        )

    @classmethod
    def create_sql(cls: type[T], **kwargs: Any) -> Fragment:
        """Generate INSERT SQL for creating a new record with RETURNING clause.

        Args:
            **kwargs: Field values for the new record

        Returns:
            Fragment containing INSERT ... RETURNING statement

        Example:
            >>> list(User.create_sql(name="Alice", email="alice@example.com"))
            ['INSERT INTO "users" ("name", "email") VALUES ($1, $2) RETURNING "id", "name", "email"', 'Alice', 'alice@example.com']
        """
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
        """Create a new record in the database.

        Args:
            connection_or_pool: Database connection or pool
            **kwargs: Field values for the new record

        Returns:
            Model instance representing the created record

        Example:
            >>> user = await User.create(pool, name="Alice", email="alice@example.com")
        """
        row = await connection_or_pool.fetchrow(*cls.create_sql(**kwargs))
        return cls.from_mapping(row)

    def insert_sql(self, exclude: FieldNamesSet = ()) -> Fragment:
        """Generate INSERT SQL for this instance.

        Args:
            exclude: Field names to exclude from the INSERT

        Returns:
            Fragment containing INSERT statement

        Example:
            >>> user = User(name="Alice", email="alice@example.com")
            >>> list(user.insert_sql())
            ['INSERT INTO "users" ("name", "email") VALUES ($1, $2)', 'Alice', 'alice@example.com']
        """
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
        """Insert this instance into the database.

        Args:
            connection_or_pool: Database connection or pool
            exclude: Field names to exclude from the INSERT

        Returns:
            Result string from the database operation
        """
        return await connection_or_pool.execute(*self.insert_sql(exclude))

    @classmethod
    def upsert_sql(
        cls,
        insert_sql: Fragment,
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> Fragment:
        """Generate UPSERT (INSERT ... ON CONFLICT DO UPDATE) SQL.

        Args:
            insert_sql: Base INSERT statement Fragment
            insert_only: Field names to exclude from the UPDATE clause
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Fragment containing INSERT ... ON CONFLICT DO UPDATE statement

        Example:
            >>> insert = user.insert_sql()
            >>> list(User.upsert_sql(insert))
            ['INSERT INTO "users" ("name", "email") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name", "email"=EXCLUDED."email"', 'Alice', 'alice@example.com']

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            excluded from the UPDATE clause, unless overridden by force_update.
        """
        # Combine insert_only parameter with auto-detected insert_only fields, but remove force_update fields
        auto_insert_only = cls.insert_only_field_names() - set(force_update)
        manual_insert_only = set(insert_only) - set(
            force_update
        )  # Remove force_update from manual insert_only too
        all_insert_only = manual_insert_only | auto_insert_only

        def generate_upsert_fragment():
            updatable_fields = cls.field_names(
                exclude=(*cls.primary_key_names, *all_insert_only)
            )
            return sql(
                " ON CONFLICT ({pks}) DO {action}",
                insert_sql=insert_sql,
                pks=sql.list(cls.primary_key_names_sql()),
                action=(
                    sql(
                        "UPDATE SET {assignments}",
                        assignments=sql.list(
                            sql("{field}=EXCLUDED.{field}", field=sql.identifier(field))
                            for field in updatable_fields
                        ),
                    )
                    if updatable_fields
                    else sql.literal("NOTHING")
                ),
            ).flatten()

        cached = cls._cached(
            ("upsert_sql", tuple(sorted(all_insert_only))),
            generate_upsert_fragment,
        )
        return Fragment([insert_sql, cached])

    async def upsert(
        self,
        connection_or_pool: Union[Connection, Pool],
        exclude: FieldNamesSet = (),
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> bool:
        """Insert or update this instance in the database.

        Args:
            connection_or_pool: Database connection or pool
            exclude: Field names to exclude from INSERT and UPDATE
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            True if the record was updated, False if it was inserted

        Example:
            >>> user = User(id=1, name="Alice", created_at=datetime.now())
            >>> # Only set created_at on INSERT, not UPDATE
            >>> was_updated = await user.upsert(pool, insert_only={'created_at'})
            >>> # Force update created_at even if it's marked insert_only in ColumnInfo
            >>> was_updated = await user.upsert(pool, force_update={'created_at'})

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            treated as insert-only and combined with the insert_only parameter,
            unless overridden by force_update.
        """
        # upsert_sql automatically handles insert_only fields from ColumnInfo
        # We only need to combine manual insert_only with exclude for the UPDATE clause
        update_exclude = set(exclude) | set(insert_only)
        query = sql(
            "{} RETURNING xmax",
            self.upsert_sql(
                self.insert_sql(exclude=exclude),
                insert_only=update_exclude,
                force_update=force_update,
            ),
        )
        result = await connection_or_pool.fetchrow(*query)
        return result["xmax"] != 0

    @classmethod
    def delete_multiple_sql(cls: type[T], rows: Iterable[T]) -> Fragment:
        """Generate DELETE SQL for multiple records.

        Args:
            rows: Model instances to delete

        Returns:
            Fragment containing DELETE statement with UNNEST-based WHERE clause

        Example:
            >>> users = [user1, user2, user3]
            >>> list(User.delete_multiple_sql(users))
            ['DELETE FROM "users" WHERE ("id") IN (SELECT * FROM UNNEST($1::UUID[]))', (uuid1, uuid2, uuid3)]
        """
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
        """Delete multiple records from the database.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to delete

        Returns:
            Result string from the database operation
        """
        return await connection_or_pool.execute(*cls.delete_multiple_sql(rows))

    @classmethod
    def insert_multiple_sql(cls: type[T], rows: Iterable[T]) -> Fragment:
        """Generate bulk INSERT SQL using UNNEST.

        This is the most efficient method for bulk inserts in PostgreSQL.

        Args:
            rows: Model instances to insert

        Returns:
            Fragment containing INSERT ... SELECT FROM UNNEST statement

        Example:
            >>> users = [User(name="Alice"), User(name="Bob")]
            >>> list(User.insert_multiple_sql(users))
            ['INSERT INTO "users" ("name", "email") SELECT * FROM UNNEST($1::TEXT[], $2::TEXT[])', ('Alice', 'Bob'), (None, None)]
        """
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
        """Generate bulk INSERT SQL using VALUES syntax.

        This method is required when your model contains array columns, because
        PostgreSQL doesn't support arrays-of-arrays (which UNNEST would require).
        Use this instead of the UNNEST method when you have array-typed fields.

        Args:
            rows: Model instances to insert

        Returns:
            Fragment containing INSERT ... VALUES statement
        """
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
        """Generate INSERT SQL template for executemany with specific chunk size.

        Args:
            chunk_size: Number of records per batch

        Returns:
            Fragment containing INSERT statement with numbered placeholders
        """

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
        """Insert multiple records using asyncpg's executemany.

        This is the most compatible but slowest bulk insert method.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to insert
        """
        args = [r.field_values() for r in rows]
        query = cls.insert_multiple_executemany_chunk_sql(1).query()[0]
        if args:
            await connection_or_pool.executemany(query, args)

    @classmethod
    async def insert_multiple_unnest(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        """Insert multiple records using PostgreSQL UNNEST.

        This is the most efficient bulk insert method for PostgreSQL.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to insert

        Returns:
            Result string from the database operation
        """
        return await connection_or_pool.execute(*cls.insert_multiple_sql(rows))

    @classmethod
    async def insert_multiple_array_safe(
        cls: type[T], connection_or_pool: Union[Connection, Pool], rows: Iterable[T]
    ) -> str:
        """Insert multiple records using VALUES syntax with chunking.

        This method is required when your model contains array columns, because
        PostgreSQL doesn't support arrays-of-arrays (which UNNEST would require).
        Data is processed in chunks to manage memory usage.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to insert

        Returns:
            Result string from the last chunk operation
        """
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
        """Insert multiple records using the configured insert_multiple_mode.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to insert

        Returns:
            Result string from the database operation

        Note:
            The actual method used depends on the insert_multiple_mode setting:
            - 'unnest': Most efficient, uses UNNEST (default)
            - 'array_safe': Uses VALUES syntax; required when model has array columns
            - 'executemany': Uses asyncpg's executemany, slowest but most compatible
        """
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
        force_update: FieldNamesSet = (),
    ) -> None:
        """Bulk upsert using asyncpg's executemany.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to upsert
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings
        """
        args = [r.field_values() for r in rows]
        query = cls.upsert_sql(
            cls.insert_multiple_executemany_chunk_sql(1),
            insert_only=insert_only,
            force_update=force_update,
        ).query()[0]
        if args:
            await connection_or_pool.executemany(query, args)

    @classmethod
    async def upsert_multiple_unnest(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> str:
        """Bulk upsert using PostgreSQL UNNEST.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to upsert
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Result string from the database operation
        """
        return await connection_or_pool.execute(
            *cls.upsert_sql(
                cls.insert_multiple_sql(rows),
                insert_only=insert_only,
                force_update=force_update,
            )
        )

    @classmethod
    async def upsert_multiple_array_safe(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> str:
        """Bulk upsert using VALUES syntax with chunking.

        This method is required when your model contains array columns, because
        PostgreSQL doesn't support arrays-of-arrays (which UNNEST would require).

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to upsert
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Result string from the last chunk operation
        """
        last = ""
        for chunk in chunked(rows, 100):
            last = await connection_or_pool.execute(
                *cls.upsert_sql(
                    cls.insert_multiple_array_safe_sql(chunk),
                    insert_only=insert_only,
                    force_update=force_update,
                )
            )
        return last

    @classmethod
    async def upsert_multiple(
        cls: type[T],
        connection_or_pool: Union[Connection, Pool],
        rows: Iterable[T],
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> str:
        """Bulk upsert (INSERT ... ON CONFLICT DO UPDATE) multiple records.

        Args:
            connection_or_pool: Database connection or pool
            rows: Model instances to upsert
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Result string from the database operation

        Example:
            >>> await User.upsert_multiple(pool, users, insert_only={'created_at'})
            >>> await User.upsert_multiple(pool, users, force_update={'created_at'})

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            treated as insert-only and combined with the insert_only parameter,
            unless overridden by force_update.
        """
        # upsert_sql automatically handles insert_only fields from ColumnInfo
        # Pass manual insert_only parameter through to the specific implementations

        if cls.insert_multiple_mode == "executemany":
            await cls.upsert_multiple_executemany(
                connection_or_pool,
                rows,
                insert_only=insert_only,
                force_update=force_update,
            )
            return "INSERT"
        elif cls.insert_multiple_mode == "array_safe":
            return await cls.upsert_multiple_array_safe(
                connection_or_pool,
                rows,
                insert_only=insert_only,
                force_update=force_update,
            )
        else:
            return await cls.upsert_multiple_unnest(
                connection_or_pool,
                rows,
                insert_only=insert_only,
                force_update=force_update,
            )

    @classmethod
    def _get_equal_ignoring_fn(
        cls: type[T], ignore: FieldNamesSet = ()
    ) -> Callable[[T, T], bool]:
        """Generate optimized function to compare instances ignoring certain fields.

        Args:
            ignore: Field names to ignore during comparison

        Returns:
            Compiled function that compares two instances, returning True if equal
        """
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
        force_update: FieldNamesSet = (),
    ) -> "ReplaceMultiplePlan[T]":
        """Plan a replace operation by comparing new data with existing records.

        This method analyzes the differences between the provided rows and existing
        database records, determining which records need to be created, updated, or deleted.

        Args:
            connection: Database connection (must support FOR UPDATE)
            rows: New data as model instances or mappings
            where: WHERE clause to limit which existing records to consider
            ignore: Field names to ignore when comparing records
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            ReplaceMultiplePlan containing the planned operations

        Example:
            >>> plan = await User.plan_replace_multiple(
            ...     conn, new_users, where=sql("department_id = {}", dept_id)
            ... )
            >>> print(f"Will create {len(plan.created)}, update {len(plan.updated)}, delete {len(plan.deleted)}")

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            treated as insert-only and combined with the insert_only parameter,
            unless overridden by force_update.
        """
        # For comparison purposes, combine auto-detected insert_only fields with manual ones
        all_insert_only = cls.insert_only_field_names() | set(insert_only)
        ignore = sorted(set(ignore) | all_insert_only)
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

        # Pass only manual insert_only and force_update to the plan
        # since upsert_multiple handles auto-detected ones
        return ReplaceMultiplePlan(
            cls, insert_only, force_update, created, updated, deleted
        )

    @classmethod
    async def replace_multiple(
        cls: type[T],
        connection: Connection,
        rows: Union[Iterable[T], Iterable[Mapping[str, Any]]],
        *,
        where: Where,
        ignore: FieldNamesSet = (),
        insert_only: FieldNamesSet = (),
        force_update: FieldNamesSet = (),
    ) -> tuple[list[T], list[T], list[T]]:
        """Replace records in the database with the provided data.

        This is a complete replace operation: records matching the WHERE clause
        that aren't in the new data will be deleted, new records will be inserted,
        and changed records will be updated.

        Args:
            connection: Database connection (must support FOR UPDATE)
            rows: New data as model instances or mappings
            where: WHERE clause to limit which existing records to consider for replacement
            ignore: Field names to ignore when comparing records
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Tuple of (created_records, updated_records, deleted_records)

        Example:
            >>> created, updated, deleted = await User.replace_multiple(
            ...     conn, new_users, where=sql("department_id = {}", dept_id)
            ... )

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            treated as insert-only and combined with the insert_only parameter,
            unless overridden by force_update.
        """
        plan = await cls.plan_replace_multiple(
            connection,
            rows,
            where=where,
            ignore=ignore,
            insert_only=insert_only,
            force_update=force_update,
        )
        await plan.execute(connection)
        return plan.cud

    @classmethod
    def _get_differences_ignoring_fn(
        cls: type[T], ignore: FieldNamesSet = ()
    ) -> Callable[[T, T], list[str]]:
        """Generate optimized function to find field differences between instances.

        Args:
            ignore: Field names to ignore during comparison

        Returns:
            Compiled function that returns list of field names that differ
        """
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
        force_update: FieldNamesSet = (),
    ) -> tuple[list[T], list[tuple[T, T, list[str]]], list[T]]:
        """Replace records and report the specific field differences for updates.

        Like replace_multiple, but provides detailed information about which
        fields changed for each updated record.

        Args:
            connection: Database connection (must support FOR UPDATE)
            rows: New data as model instances or mappings
            where: WHERE clause to limit which existing records to consider
            ignore: Field names to ignore when comparing records
            insert_only: Field names that should only be set on INSERT, not UPDATE
            force_update: Field names to force include in UPDATE clause, overriding insert_only settings

        Returns:
            Tuple of (created_records, update_triples, deleted_records)
            where update_triples contains (old_record, new_record, changed_field_names)

        Example:
            >>> created, updates, deleted = await User.replace_multiple_reporting_differences(
            ...     conn, new_users, where=sql("department_id = {}", dept_id)
            ... )
            >>> for old, new, fields in updates:
            ...     print(f"Updated {old.name}: changed {', '.join(fields)}")

        Note:
            Fields marked with ColumnInfo(insert_only=True) are automatically
            treated as insert-only and combined with the insert_only parameter,
            unless overridden by force_update.
        """
        # For comparison purposes, combine auto-detected insert_only fields with manual ones
        all_insert_only = cls.insert_only_field_names() | set(insert_only)
        ignore = sorted(set(ignore) | all_insert_only)
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
                force_update=force_update,
            )
        if deleted:
            await cls.delete_multiple(connection, deleted)

        return created, updated_triples, deleted


@dataclass
class ReplaceMultiplePlan(Generic[T]):
    model_class: type[T]
    insert_only: FieldNamesSet
    force_update: FieldNamesSet
    created: list[T]
    updated: list[T]
    deleted: list[T]

    @property
    def cud(self) -> tuple[list[T], list[T], list[T]]:
        """Get the create, update, delete lists as a tuple.

        Returns:
            Tuple of (created, updated, deleted) record lists
        """
        return (self.created, self.updated, self.deleted)

    async def execute_upserts(self, connection: Connection) -> None:
        """Execute the upsert operations (creates and updates).

        Args:
            connection: Database connection
        """
        if self.created or self.updated:
            await self.model_class.upsert_multiple(
                connection,
                (*self.created, *self.updated),
                insert_only=self.insert_only,
                force_update=self.force_update,
            )

    async def execute_deletes(self, connection: Connection) -> None:
        """Execute the delete operations.

        Args:
            connection: Database connection
        """
        if self.deleted:
            await self.model_class.delete_multiple(connection, self.deleted)

    async def execute(self, connection: Connection) -> None:
        """Execute all planned operations (upserts then deletes).

        Args:
            connection: Database connection
        """
        await self.execute_upserts(connection)
        await self.execute_deletes(connection)


def chunked(lst, n):
    """Split an iterable into chunks of size n.

    Args:
        lst: Iterable to chunk
        n: Chunk size

    Yields:
        Lists of up to n items from the input

    Example:
        >>> list(chunked([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    if type(lst) is not list:
        lst = list(lst)
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
