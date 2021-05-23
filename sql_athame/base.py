import dataclasses
import json
import re
import string
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

from typing_extensions import Literal

from .escape import escape


@dataclasses.dataclass(eq=False)
class Placeholder:
    __slots__ = ["name", "value"]
    name: str
    value: Any


@dataclasses.dataclass(frozen=True)
class Slot:
    __slots__ = ["name"]
    name: str


Part = Union[str, Placeholder, Slot, "Fragment"]
FlatPart = Union[str, Placeholder, Slot]


newline_whitespace_re = re.compile(r"\s*\n\s*")
auto_numbered_re = re.compile(r"[A-Za-z0-9_]")


def auto_numbered(field_name):
    return not auto_numbered_re.match(field_name)


def process_slot_value(
    name: str,
    value: Any,
    placeholders: Dict[str, Placeholder],
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
    parts: List[Part]

    def flatten_into(self, parts: List[FlatPart]) -> None:
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
                    f"  process_slot_value({repr(part.name)}, slots[{repr(part.name)}], placeholders),"
                )
            elif isinstance(part, str):
                func.append(f"  {repr(part)},")
            else:
                env[f"part_{i}"] = part
                func.append(f"  part_{i},")
        func += [" ])"]
        exec("\n".join(func), env)
        return env["compiled"]  # type: ignore

    def flatten(self) -> "Fragment":
        parts: List[FlatPart] = []
        self.flatten_into(parts)
        out_parts: List[Part] = []
        for part in parts:
            if isinstance(part, str) and out_parts and isinstance(out_parts[-1], str):
                out_parts[-1] += part
            else:
                out_parts.append(part)
        return Fragment(out_parts)

    def fill(self, **kwargs: Any) -> "Fragment":
        parts: List[Part] = []
        self.flatten_into(cast(List[FlatPart], parts))
        placeholders: Dict[str, Placeholder] = {}
        for i, part in enumerate(parts):
            if isinstance(part, Slot):
                parts[i] = process_slot_value(
                    part.name, kwargs[part.name], placeholders
                )
        return Fragment(parts)

    @overload
    def prep_query(
        self, allow_slots: Literal[True]
    ) -> Tuple[str, List[Union[Placeholder, Slot]]]:
        ...  # pragma: no cover

    @overload
    def prep_query(
        self, allow_slots: Literal[False] = False
    ) -> Tuple[str, List[Placeholder]]:
        ...  # pragma: no cover

    def prep_query(self, allow_slots: bool = False) -> Tuple[str, List[Any]]:
        parts: List[FlatPart] = []
        self.flatten_into(parts)
        args: List[Union[Placeholder, Slot]] = []
        placeholder_ids: Dict[Placeholder, int] = {}
        slot_ids: Dict[Slot, int] = {}
        out_parts: List[str] = []
        for part in parts:
            if isinstance(part, Slot):
                if not allow_slots:
                    raise ValueError(f"Unfilled slot: {repr(part.name)}")
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

    def query(self) -> Tuple[str, List[Any]]:
        query, args = self.prep_query()
        placeholder_values = [arg.value for arg in args]
        return query, placeholder_values

    def prepare(self) -> Tuple[str, Callable[..., List[Any]]]:
        query, args = self.prep_query(allow_slots=True)
        env = dict()
        func = [
            "def generate_args(**kwargs):",
            " return [",
        ]
        for i, arg in enumerate(args):
            if isinstance(arg, Slot):
                func.append(f"  kwargs[{repr(arg.name)}],")
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
        parts: List[Part] = []
        placeholders: Dict[str, Placeholder] = {}
        next_auto_field = 0
        for literal_text, field_name, format_spec, conversion in fmtr.parse(fmt):
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
    def all(self, parts: Iterable[Fragment]) -> Fragment:
        ...  # pragma: no cover

    @overload
    def all(self, *parts: Fragment) -> Fragment:
        ...  # pragma: no cover

    def all(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "AND", "TRUE")

    @overload
    def any(self, parts: Iterable[Fragment]) -> Fragment:
        ...  # pragma: no cover

    @overload
    def any(self, *parts: Fragment) -> Fragment:
        ...  # pragma: no cover

    def any(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return any_all(list(parts), "OR", "FALSE")

    @overload
    def list(self, parts: Iterable[Fragment]) -> Fragment:
        ...  # pragma: no cover

    @overload
    def list(self, *parts: Fragment) -> Fragment:
        ...  # pragma: no cover

    def list(self, *parts) -> Fragment:  # type: ignore
        if parts and not isinstance(parts[0], Fragment):
            parts = parts[0]
        return Fragment(list(join_parts(parts, infix=", ")))

    @staticmethod
    def unnest(data: Iterable[Sequence[Any]], types: Iterable[str]) -> Fragment:
        nested = list(nest_for_type(x, t) for x, t in zip(zip(*data), types))
        if not nested:
            nested = list(nest_for_type([], t) for t in types)
        return Fragment(["UNNEST(", sql.list(nested), ")"])


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


def any_all(frags: List[Fragment], op: str, base_case: str) -> Fragment:
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
