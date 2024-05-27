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
    return not auto_numbered_re.match(field_name)


def process_slot_value(
    name: str,
    value: Any,
    placeholders: dict[str, Placeholder],
) -> Union["Fragment", Placeholder]:
    if isinstance(value, Fragment):
        return value
    else:
        if name not in placeholders:
            placeholders[name] = Placeholder(name, value)
        return placeholders[name]


@dataclasses.dataclass
class Fragment:
    __slots__ = ["parts"]
    parts: list[Part]

    def flatten_into(self, parts: list[FlatPart]) -> None:
        for part in self.parts:
            if isinstance(part, Fragment):
                part.flatten_into(parts)
            else:
                parts.append(part)

    def compile(self) -> Callable[..., "Fragment"]:
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
        query, args = self.prep_query()
        placeholder_values = [arg.value for arg in args]
        return query, placeholder_values

    def sqlalchemy_text(self) -> Any:
        return sqlalchemy_text_from_fragment(self)

    def prepare(self) -> tuple[str, Callable[..., list[Any]]]:
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
        sql, args = self.query()
        return iter((sql, *args))

    def join(self, parts: Iterable["Fragment"]) -> "Fragment":
        return Fragment(list(join_parts(parts, infix=self)))


class SQLFormatter:
    def __call__(
        self, fmt: str, *args: Any, preserve_formatting: bool = False, **kwargs: Any
    ) -> Fragment:
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
        placeholder = Placeholder("value", value)
        return Fragment([placeholder])

    @staticmethod
    def escape(value: Any) -> Fragment:
        return lit(escape(value))

    @staticmethod
    def slot(name: str) -> Fragment:
        return Fragment([Slot(name)])

    @staticmethod
    def literal(text: str) -> Fragment:
        return Fragment([text])

    @staticmethod
    def identifier(name: str, prefix: Optional[str] = None) -> Fragment:
        if prefix:
            return lit(f"{quote_identifier(prefix)}.{quote_identifier(name)}")
        else:
            return lit(f"{quote_identifier(name)}")

    @overload
    def all(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def all(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def all(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "AND", "TRUE")

    @overload
    def any(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def any(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def any(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "OR", "FALSE")

    @overload
    def list(self, parts: Iterable[Fragment]) -> Fragment: ...  # pragma: no cover

    @overload
    def list(self, *parts: Fragment) -> Fragment: ...  # pragma: no cover

    def list(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return Fragment(list(join_parts(parts, infix=", ")))

    def unnest(self, data: Iterable[Sequence[Any]], types: Iterable[str]) -> Fragment:
        nested = [nest_for_type(x, t) for x, t in zip(zip(*data), types)]
        if not nested:
            nested = [nest_for_type([], t) for t in types]
        return Fragment(["UNNEST(", self.list(nested), ")"])


sql = SQLFormatter()


json_types = ("JSON", "JSONB")


def is_json_type(typename: str) -> bool:
    return typename.upper() in json_types


def nest_for_type(data: Sequence[Any], typename: str) -> Fragment:
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
    return Fragment([text])


def any_all(frags: list[Fragment], op: str, base_case: str) -> Fragment:
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
    if prefix:
        yield prefix
    for i, part in enumerate(parts):
        if i:
            yield infix
        yield part
    if suffix:
        yield suffix


def quote_identifier(name: str) -> str:
    quoted = name.replace('"', '""')
    return f'"{quoted}"'
