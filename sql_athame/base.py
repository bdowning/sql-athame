import dataclasses
import json
import re
import string
from collections.abc import Iterable, Iterator, Sequence
from typing import (
    Any,
    Callable,
    Optional,
    Union,
    cast,
    overload,
)

from typing_extensions import Literal

from .escape import escape
from .sqlalchemy import sqlalchemy_text_from_fragment
from .types import FlatPart, Part, Placeholder, Slot

newline_whitespace_re = re.compile(r"\s*\n\s*")
auto_numbered_re = re.compile(r"[A-Za-z0-9_]")


def auto_numbered(field_name):
    """Check if a field name should be auto-numbered.

    Args:
        field_name: The field name to check

    Returns:
        True if the field name should be auto-numbered (doesn't start with alphanumeric)
    """
    return not auto_numbered_re.match(field_name)


def process_slot_value(
    name: str,
    value: Any,
    placeholders: dict[str, Placeholder],
) -> Union["Fragment", Placeholder]:
    """Process a slot value during fragment compilation.

    Args:
        name: The slot name
        value: The value to process
        placeholders: Dictionary of existing placeholders

    Returns:
        Either a Fragment if value is a Fragment, or a Placeholder
    """
    if isinstance(value, Fragment):
        return value
    else:
        if name not in placeholders:
            placeholders[name] = Placeholder(name, value)
        return placeholders[name]


@dataclasses.dataclass
class Fragment:
    """Core SQL fragment class representing a piece of SQL with placeholders.

    A Fragment contains SQL text and placeholders that can be combined with other
    fragments to build complex queries. Fragments automatically handle parameter
    binding and can be rendered to parameterized queries suitable for database drivers.

    Attributes:
        parts: List of SQL parts (strings, placeholders, slots, or other fragments)

    Example:
        >>> from sql_athame import sql
        >>> frag = sql("SELECT * FROM users WHERE id = {}", 42)
        >>> query, params = frag.query()
        >>> query
        'SELECT * FROM users WHERE id = $1'
        >>> params
        [42]

        >>> # Fragments can be combined
        >>> where_clause = sql("active = {}", True)
        >>> full_query = sql("SELECT * FROM users WHERE id = {} AND {}", 42, where_clause)
        >>> list(full_query)
        ['SELECT * FROM users WHERE id = $1 AND active = $2', 42, True]
    """

    __slots__ = ["parts"]
    parts: list[Part]

    def flatten_into(self, parts: list[FlatPart]) -> None:
        """Recursively flatten this fragment into a list of flat parts.

        This method traverses nested fragments and adds all parts to the provided list.
        String parts are not combined here - that's done in flatten().

        Args:
            parts: List to append flattened parts to
        """
        for part in self.parts:
            if isinstance(part, Fragment):
                part.flatten_into(parts)
            else:
                parts.append(part)

    def compile(self) -> Callable[..., "Fragment"]:
        """Create an optimized function for filling slots in this fragment.

        Returns a compiled function that when called with **kwargs will create a new
        Fragment equivalent to calling self.fill(**kwargs). The compilation process
        does as much work as possible up front, making repeated slot filling much faster.

        Returns:
            Function that takes **kwargs and returns a Fragment with slots filled

        Example:
            >>> template = sql("SELECT * FROM users WHERE name = {name} AND age > {age}")
            >>> compiled_template = template.compile()
            >>> query1 = compiled_template(name="Alice", age=25)
            >>> query2 = compiled_template(name="Bob", age=30)
        """
        flattened = self.flatten()
        env = dict(
            process_slot_value=process_slot_value,
            Fragment=Fragment,
        )
        func = [
            "def compiled(**slots):",
            " placeholders = {}",
            " return Fragment([",
        ]
        for i, part in enumerate(flattened.parts):
            if isinstance(part, Slot):
                func.append(
                    f"  process_slot_value({part.name!r}, slots[{part.name!r}], placeholders),"
                )
            elif isinstance(part, str):
                func.append(f"  {part!r},")
            else:
                env[f"part_{i}"] = part
                func.append(f"  part_{i},")
        func += [" ])"]
        exec("\n".join(func), env)
        return env["compiled"]  # type: ignore

    def flatten(self) -> "Fragment":
        """Create a flattened version of this fragment.

        Recursively flattens all nested fragments and combines adjacent string parts
        into single strings for efficiency.

        Returns:
            New Fragment with no nested fragments and adjacent strings combined
        """
        parts: list[FlatPart] = []
        self.flatten_into(parts)
        out_parts: list[Part] = []
        for part in parts:
            if isinstance(part, str) and out_parts and isinstance(out_parts[-1], str):
                out_parts[-1] += part
            else:
                out_parts.append(part)
        return Fragment(out_parts)

    def fill(self, **kwargs: Any) -> "Fragment":
        """Create a new fragment by filling any empty slots with provided values.

        Searches for Slot objects in this fragment and replaces them with the
        corresponding values from kwargs. If a value is a Fragment, it's substituted
        in-place; otherwise it becomes a placeholder.

        Args:
            **kwargs: Named values to fill into slots

        Returns:
            New Fragment with slots filled

        Example:
            >>> template = sql("SELECT * FROM {table} WHERE id = {id}")
            >>> query = template.fill(table=sql.identifier("users"), id=42)
            >>> list(query)
            ['SELECT * FROM "users" WHERE id = $1', 42]
        """
        parts: list[Part] = []
        self.flatten_into(cast(list[FlatPart], parts))
        placeholders: dict[str, Placeholder] = {}
        for i, part in enumerate(parts):
            if isinstance(part, Slot):
                parts[i] = process_slot_value(
                    part.name, kwargs[part.name], placeholders
                )
        return Fragment(parts)

    @overload
    def prep_query(
        self, allow_slots: Literal[True]
    ) -> tuple[str, list[Union[Placeholder, Slot]]]: ...  # pragma: no cover

    @overload
    def prep_query(
        self, allow_slots: Literal[False] = False
    ) -> tuple[str, list[Placeholder]]: ...  # pragma: no cover

    def prep_query(self, allow_slots: bool = False) -> tuple[str, list[Any]]:
        """Prepare the fragment for query execution.

        Flattens the fragment and converts placeholders to numbered parameters ($1, $2, etc.)
        suitable for database drivers like asyncpg.

        Args:
            allow_slots: If True, allows unfilled slots; if False, raises ValueError for unfilled slots

        Returns:
            Tuple of (query_string, parameter_objects)

        Raises:
            ValueError: If allow_slots is False and there are unfilled slots
        """
        parts: list[FlatPart] = []
        self.flatten_into(parts)
        args: list[Union[Placeholder, Slot]] = []
        placeholder_ids: dict[Placeholder, int] = {}
        slot_ids: dict[Slot, int] = {}
        out_parts: list[str] = []
        for part in parts:
            if isinstance(part, Slot):
                if not allow_slots:
                    raise ValueError(f"Unfilled slot: {part.name!r}")
                if part not in slot_ids:
                    args.append(part)
                    slot_ids[part] = len(args)
                out_parts.append(f"${slot_ids[part]}")
            elif isinstance(part, Placeholder):
                if part not in placeholder_ids:
                    args.append(part)
                    placeholder_ids[part] = len(args)
                out_parts.append(f"${placeholder_ids[part]}")
            else:
                assert isinstance(part, str)
                out_parts.append(part)
        return "".join(out_parts).strip(), args

    def query(self) -> tuple[str, list[Any]]:
        """Render the fragment into a query string and parameter list.

        Returns:
            Tuple of (query_string, parameter_values) ready for database execution

        Raises:
            ValueError: If there are any unfilled slots

        Example:
            >>> frag = sql("SELECT * FROM users WHERE id = {}", 42)
            >>> frag.query()
            ('SELECT * FROM users WHERE id = $1', [42])
        """
        query, args = self.prep_query()
        placeholder_values = [arg.value for arg in args]
        return query, placeholder_values

    def sqlalchemy_text(self) -> Any:
        """Convert this fragment to a SQLAlchemy TextClause.

        Renders the fragment into a SQLAlchemy TextClause with bound parameters.
        Placeholder values will be bound with bindparams. Unfilled slots will be
        included as unbound parameters.

        Returns:
            SQLAlchemy TextClause object

        Raises:
            ImportError: If SQLAlchemy is not installed

        Example:
            >>> frag = sql("SELECT * FROM users WHERE id = {}", 42)
            >>> text_clause = frag.sqlalchemy_text()
            >>> # Can be used with SQLAlchemy engine.execute(text_clause)
        """
        return sqlalchemy_text_from_fragment(self)

    def prepare(self) -> tuple[str, Callable[..., list[Any]]]:
        """Prepare fragment for use with prepared statements.

        Returns a query string and a function that generates parameter lists.
        The query string contains numbered placeholders, and the function takes
        **kwargs for any unfilled slots and returns the complete parameter list.

        Returns:
            Tuple of (query_string, parameter_generator_function)

        Example:
            >>> template = sql("UPDATE users SET name={name}, age={age} WHERE id < {}", 100)
            >>> query, param_func = template.prepare()
            >>> query
            'UPDATE users SET name=$1, age=$2 WHERE id < $3'
            >>> param_func(name="Alice", age=25)
            ['Alice', 25, 100]
            >>> param_func(name="Bob", age=30)
            ['Bob', 30, 100]
        """
        query, args = self.prep_query(allow_slots=True)
        env = {}
        func = [
            "def generate_args(**kwargs):",
            " return [",
        ]
        for i, arg in enumerate(args):
            if isinstance(arg, Slot):
                func.append(f"  kwargs[{arg.name!r}],")
            else:
                env[f"value_{i}"] = arg.value
                func.append(f"  value_{i},")
        func += [" ]"]
        exec("\n".join(func), env)
        return query, env["generate_args"]  # type: ignore

    def __iter__(self) -> Iterator[Any]:
        """Make Fragment iterable for use with asyncpg and similar drivers.

        Returns an iterator that yields the query string followed by all parameter
        values. This matches the (query, *args) calling convention of asyncpg.

        Yields:
            Query string, then each parameter value

        Example:
            >>> frag = sql("SELECT * FROM users WHERE id = {} AND name = {}", 42, "Alice")
            >>> list(frag)
            ['SELECT * FROM users WHERE id = $1 AND name = $2', 42, 'Alice']
            >>> # Can be used directly with asyncpg
            >>> await conn.fetch(*frag)
        """
        sql, args = self.query()
        return iter((sql, *args))

    def join(self, parts: Iterable["Fragment"]) -> "Fragment":
        """Join multiple fragments using this fragment as a separator.

        Creates a new fragment by joining the provided fragments with this fragment
        as the separator between them.

        Args:
            parts: Iterable of Fragment objects to join

        Returns:
            New Fragment with parts joined by this fragment

        Example:
            >>> separator = sql(" AND ")
            >>> conditions = [sql("a = {}", 1), sql("b = {}", 2), sql("c = {}", 3)]
            >>> result = separator.join(conditions)
            >>> list(result)
            ['a = $1 AND b = $2 AND c = $3', 1, 2, 3]

            >>> # More commonly used for CASE statements
            >>> clauses = [sql("WHEN {} THEN {}", x, y) for x, y in [("a", 1), ("b", 2)]]
            >>> case = sql("CASE {clauses} END", clauses=sql(" ").join(clauses))
        """
        return Fragment(list(join_parts(parts, infix=self)))


class SQLFormatter:
    """Main SQL formatting class providing the sql() function and utility methods.

    This class is instantiated as the global 'sql' object that provides the primary
    interface for building SQL fragments. It supports format string syntax with
    placeholders and provides utility methods for common SQL operations.
    """

    def __call__(
        self, fmt: str, *args: Any, preserve_formatting: bool = False, **kwargs: Any
    ) -> Fragment:
        """Create a SQL Fragment from a format string with placeholders.

        The format string contains literal SQL and may contain positional references
        marked by {} and named references marked by {name}. Positional references
        must have matching arguments in *args. Named references may have matching
        arguments in **kwargs; if not provided, they remain as named slots to be
        filled later.

        If a referenced argument is a Fragment, it is substituted into the SQL
        along with its embedded placeholders. Otherwise, it becomes a placeholder value.

        Args:
            fmt: SQL format string with {} placeholders
            *args: Positional arguments for {} placeholders
            preserve_formatting: If True, preserve whitespace; if False, normalize whitespace
            **kwargs: Named arguments for {name} placeholders

        Returns:
            Fragment containing the SQL with placeholders

        Raises:
            ValueError: If there are unfilled positional arguments

        Example:
            >>> sql("SELECT * FROM users WHERE id = {}", 42)
            Fragment(['SELECT * FROM users WHERE id = ', Placeholder('0', 42)])

            >>> sql("SELECT * FROM users WHERE id = {id} AND name = {name}", id=42, name="Alice")
            Fragment(['SELECT * FROM users WHERE id = ', Placeholder('id', 42), ' AND name = ', Placeholder('name', 'Alice')])

            >>> # Fragments can be embedded
            >>> where_clause = sql("active = {}", True)
            >>> sql("SELECT * FROM users WHERE {}", where_clause)
        """
        if not preserve_formatting:
            fmt = newline_whitespace_re.sub(" ", fmt)
        fmtr = string.Formatter()
        parts: list[Part] = []
        placeholders: dict[str, Placeholder] = {}
        next_auto_field = 0
        for literal_text, field_name, _format_spec, _conversion in fmtr.parse(fmt):
            parts.append(literal_text)
            if field_name is not None:
                if auto_numbered(field_name):
                    field_name = f"{next_auto_field}{field_name}"
                    next_auto_field += 1
                try:
                    value = fmtr.get_field(field_name, args, kwargs)[0]
                except IndexError as e:
                    raise ValueError("unfilled positional argument") from e
                except KeyError:
                    value = Slot(field_name)
                if isinstance(value, Fragment) or isinstance(value, Slot):
                    parts.append(value)
                else:
                    if field_name not in placeholders:
                        placeholders[field_name] = Placeholder(field_name, value)
                    parts.append(placeholders[field_name])
        return Fragment(parts)

    @staticmethod
    def value(value: Any) -> Fragment:
        """Create a Fragment with a single placeholder value.

        Equivalent to sql("{}", value) but more explicit.

        Args:
            value: The value to create a placeholder for

        Returns:
            Fragment containing a single placeholder

        Example:
            >>> sql.value(42)
            Fragment([Placeholder('value', 42)])
        """
        placeholder = Placeholder("value", value)
        return Fragment([placeholder])

    @staticmethod
    def escape(value: Any) -> Fragment:
        """Create a Fragment with a value escaped and embedded into the SQL.

        Unlike placeholders, escaped values are embedded directly into the SQL text.
        Types currently supported are strings, floats, ints, UUIDs, None, and
        sequences of the above. Use with caution and only for trusted values.

        Args:
            value: The value to escape and embed

        Returns:
            Fragment with the escaped value as literal SQL

        Example:
            >>> list(sql("SELECT * FROM tbl WHERE qty = ANY({})", sql.escape([1, 3, 5])))
            ['SELECT * FROM tbl WHERE qty = ANY(ARRAY[1, 3, 5])']

            >>> # Compare to placeholder version:
            >>> list(sql("SELECT * FROM tbl WHERE qty = ANY({})", [1, 3, 5]))
            ['SELECT * FROM tbl WHERE qty = ANY($1)', [1, 3, 5]]

        Note:
            Burning invariant values into the query can potentially help the query optimizer.
        """
        return lit(escape(value))

    @staticmethod
    def slot(name: str) -> Fragment:
        """Create a Fragment with a single empty slot.

        Equivalent to sql("{name}") but more explicit.

        Args:
            name: The name of the slot

        Returns:
            Fragment containing a single slot

        Example:
            >>> template = sql("SELECT * FROM users WHERE {}", sql.slot("condition"))
            >>> query = template.fill(condition=sql("active = {}", True))
        """
        return Fragment([Slot(name)])

    @staticmethod
    def literal(text: str) -> Fragment:
        """Create a Fragment with literal SQL text.

        No substitution of any kind is performed. Be very careful of SQL injection
        when using this method.

        Args:
            text: Raw SQL text to include

        Returns:
            Fragment containing the literal SQL

        Warning:
            Only use with trusted SQL text to avoid SQL injection vulnerabilities.

        Example:
            >>> sql.literal("ORDER BY created_at DESC")
            Fragment(['ORDER BY created_at DESC'])
        """
        return Fragment([text])

    @staticmethod
    def identifier(name: str, prefix: Optional[str] = None) -> Fragment:
        """Create a Fragment with a quoted SQL identifier.

        Creates a properly quoted identifier name, optionally with a dotted prefix
        for schema or table qualification.

        Args:
            name: The identifier name to quote
            prefix: Optional prefix (schema, table, etc.)

        Returns:
            Fragment containing the quoted identifier

        Example:
            >>> list(sql("SELECT {col} FROM {table}",
            ...           col=sql.identifier("user_name"),
            ...           table=sql.identifier("users", prefix="public")))
            ['SELECT "user_name" FROM "public"."users"']
        """
        if prefix:
            return lit(f"{quote_identifier(prefix)}.{quote_identifier(name)}")
        else:
            return lit(f"{quote_identifier(name)}")

    @overload
    def all(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def all(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def all(self, *parts) -> Fragment:  # type: ignore
        """Join fragments with AND, returning TRUE if no parts provided.

        Creates a SQL Fragment joining the fragments in parts together with AND.
        If parts is empty, returns TRUE. Each fragment is wrapped in parentheses.

        Args:
            *parts: Fragment objects to join, or single iterable of fragments

        Returns:
            Fragment containing the AND-joined conditions

        Example:
            >>> where = [sql("a = {}", 42), sql("x <> {}", "foo")]
            >>> list(sql("SELECT * FROM tbl WHERE {}", sql.all(where)))
            ['SELECT * FROM tbl WHERE (a = $1) AND (x <> $2)', 42, 'foo']
            >>> list(sql("SELECT * FROM tbl WHERE {}", sql.all([])))
            ['SELECT * FROM tbl WHERE TRUE']
        """
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "AND", "TRUE")

    @overload
    def any(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def any(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def any(self, *parts) -> Fragment:  # type: ignore
        """Join fragments with OR, returning FALSE if no parts provided.

        Creates a SQL Fragment joining the fragments in parts together with OR.
        If parts is empty, returns FALSE. Each fragment is wrapped in parentheses.

        Args:
            *parts: Fragment objects to join, or single iterable of fragments

        Returns:
            Fragment containing the OR-joined conditions

        Example:
            >>> where = [sql("a = {}", 42), sql("x <> {}", "foo")]
            >>> list(sql("SELECT * FROM tbl WHERE {}", sql.any(where)))
            ['SELECT * FROM tbl WHERE (a = $1) OR (x <> $2)', 42, 'foo']
            >>> list(sql("SELECT * FROM tbl WHERE {}", sql.any([])))
            ['SELECT * FROM tbl WHERE FALSE']
        """
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "OR", "FALSE")

    @overload
    def list(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def list(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def list(self, *parts) -> Fragment:  # type: ignore
        """Join fragments with commas.

        Creates a SQL Fragment joining the fragments in parts together with commas.
        Commonly used for column lists, value lists, etc.

        Args:
            *parts: Fragment objects to join, or single iterable of fragments

        Returns:
            Fragment containing the comma-separated fragments

        Example:
            >>> cols = [sql.identifier("id"), sql.identifier("name"), sql.identifier("email")]
            >>> list(sql("SELECT {} FROM users", sql.list(cols)))
            ['SELECT "id", "name", "email" FROM users']
        """
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return Fragment(list(join_parts(parts, infix=", ")))

    def unnest(self, data: Iterable[Sequence[Any]], types: Iterable[str]) -> Fragment:
        """Create a Fragment containing an UNNEST expression with associated data.

        The data is specified as tuples (in the "database columns" sense) in data,
        and the PostgreSQL types must be specified in types. The data is transposed
        into the correct form for UNNEST and embedded as placeholders.

        Args:
            data: Iterable of sequences, where each sequence represents a row
            types: Iterable of PostgreSQL type names for each column

        Returns:
            Fragment containing UNNEST expression with typed array placeholders

        Example:
            >>> list(sql("SELECT * FROM {}", sql.unnest([("a", 1), ("b", 2), ("c", 3)], ["text", "integer"])))
            ['SELECT * FROM UNNEST($1::text[], $2::integer[])', ('a', 'b', 'c'), (1, 2, 3)]

            >>> # Useful for bulk operations
            >>> users_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
            >>> insert_query = sql("INSERT INTO users (name, age) SELECT * FROM {}",
            ...                   sql.unnest(users_data, ["text", "integer"]))
        """
        nested = [nest_for_type(x, t) for x, t in zip(zip(*data), types)]
        if not nested:
            nested = [nest_for_type([], t) for t in types]
        return Fragment(["UNNEST(", self.list(nested), ")"])


sql = SQLFormatter()
"""Global SQLFormatter instance providing the main sql() function and utilities.

This is the primary interface for creating SQL fragments. Use it to:
- Create fragments: sql("SELECT * FROM users WHERE id = {}", user_id)
- Join fragments: sql.all([condition1, condition2])
- Create identifiers: sql.identifier("table_name")
- And much more

See SQLFormatter class documentation for all available methods.
"""


json_types = ("JSON", "JSONB")


def is_json_type(typename: str) -> bool:
    """Check if a type name represents a JSON type.

    Args:
        typename: PostgreSQL type name to check

    Returns:
        True if typename is JSON or JSONB (case insensitive)
    """
    return typename.upper() in json_types


def nest_for_type(data: Sequence[Any], typename: str) -> Fragment:
    """Create a typed array fragment for UNNEST operations.

    Converts a sequence of data into a properly typed PostgreSQL array fragment.
    Handles JSON/JSONB types specially by converting objects to JSON strings.

    Args:
        data: Sequence of values to convert to array
        typename: PostgreSQL type name for the array

    Returns:
        Fragment containing a typed array placeholder

    Note:
        For JSON/JSONB types, non-string values are converted to JSON strings
        to work around asyncpg limitations.
    """
    if is_json_type(typename):
        # https://github.com/MagicStack/asyncpg/issues/345

        # KLUDGE - this doesn't work for trying to store literal
        # strings when autoconverting; None is treated as SQL NULL
        processed_data = [
            x if x is None or isinstance(x, str) else json.dumps(x) for x in data
        ]
        return Fragment(
            [Placeholder("data", processed_data), f"::TEXT[]::{typename}[]"]
        )
    else:
        return Fragment([Placeholder("data", data), f"::{typename}[]"])


def lit(text: str) -> Fragment:
    """Create a Fragment containing literal SQL text.

    Convenience function equivalent to Fragment([text]).

    Args:
        text: Literal SQL text

    Returns:
        Fragment containing the literal text
    """
    return Fragment([text])


def any_all(frags: list[Fragment], op: str, base_case: str) -> Fragment:
    """Join fragments with a logical operator, with a base case for empty lists.

    Used by sql.all() and sql.any() to implement AND/OR joining with appropriate
    base cases (TRUE for AND, FALSE for OR).

    Args:
        frags: List of fragments to join
        op: Operator to use for joining ("AND" or "OR")
        base_case: Value to return if frags is empty ("TRUE" or "FALSE")

    Returns:
        Fragment with fragments joined by operator, or base case if empty
    """
    if not frags:
        return lit(base_case)
    parts = join_parts(frags, prefix="(", infix=f") {op} (", suffix=")")
    return Fragment(list(parts))


def join_parts(
    parts: Iterable[Part],
    infix: Part,
    prefix: Optional[Part] = None,
    suffix: Optional[Part] = None,
) -> Iterator[Part]:
    """Join parts with a separator, optionally adding prefix and suffix.

    Generator function that yields parts with infix separators between them,
    and optional prefix/suffix parts.

    Args:
        parts: Parts to join
        infix: Separator to place between parts
        prefix: Optional part to yield first
        suffix: Optional part to yield last

    Yields:
        Parts with separators, prefix, and suffix as appropriate
    """
    if prefix:
        yield prefix
    for i, part in enumerate(parts):
        if i:
            yield infix
        yield part
    if suffix:
        yield suffix


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier with double quotes, escaping internal quotes.

    Args:
        name: Identifier name to quote

    Returns:
        Quoted identifier with internal quotes escaped

    Example:
        >>> quote_identifier("user_name")
        '"user_name"'
        >>> quote_identifier('table"with"quotes')
        '"table""with""quotes"'
    """
    quoted = name.replace('"', '""')
    return f'"{quoted}"'
