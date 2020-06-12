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
)

from .escape import escape


@dataclasses.dataclass(eq=False)
class Placeholder:
    __slots__ = ["name"]
    name: str

    def __repr__(self):
        return f"Placeholder(id={id(self)}, name={repr(self.name)})"


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
    values: Dict[Placeholder, Any],
    placeholders: Dict[str, Placeholder],
) -> Union["Fragment", Placeholder]:
    if isinstance(value, Fragment):
        return value
    else:
        if name not in placeholders:
            placeholders[name] = Placeholder(name)
            values[placeholders[name]] = value
        return placeholders[name]


@dataclasses.dataclass
class Fragment:
    __slots__ = ["parts", "values"]
    parts: List[Part]
    values: Dict[Placeholder, Any]

    def flatten_into(self, parts: List[FlatPart], values: Dict[Placeholder, Any]):
        for part in self.parts:
            if isinstance(part, Fragment):
                part.flatten_into(parts, values)
            elif isinstance(part, Placeholder):
                parts.append(part)
                values[part] = self.values[part]
            else:
                parts.append(part)

    def compile(self) -> Callable[..., "Fragment"]:
        flattened = self.flatten()
        env = dict(
            in_values=flattened.values,
            process_slot_value=process_slot_value,
            Fragment=Fragment,
        )
        func = [
            "def compiled(**slots):",
            " values = in_values.copy()",
            " placeholders = {}",
            " return Fragment([",
        ]
        for i, part in enumerate(flattened.parts):
            if isinstance(part, Slot):
                func.append(
                    f"  process_slot_value({repr(part.name)}, slots[{repr(part.name)}], values, placeholders),"
                )
            elif isinstance(part, str):
                func.append(f"  {repr(part)},")
            else:
                env[f"part_{i}"] = part
                func.append(f"  part_{i},")
        func += [" ], values)"]
        exec("\n".join(func), env)
        return env["compiled"]  # type: ignore

    def flatten(self) -> "Fragment":
        parts: List[FlatPart] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(parts, values)
        out_parts: List[Part] = []
        for part in parts:
            if isinstance(part, str) and out_parts and isinstance(out_parts[-1], str):
                out_parts[-1] += part
            else:
                out_parts.append(part)
        return Fragment(out_parts, values)

    def fill(self, **kwargs) -> "Fragment":
        parts: List[Part] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(cast(List[FlatPart], parts), values)
        placeholders: Dict[str, Placeholder] = {}
        for i, part in enumerate(parts):
            if isinstance(part, Slot):
                parts[i] = process_slot_value(
                    part.name, kwargs[part.name], values, placeholders
                )
        return Fragment(parts, values)

    def prep_query(self, allow_slots=False):
        parts: List[FlatPart] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(parts, values)
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
        return "".join(out_parts).strip(), values, args

    def query(self) -> Tuple[str, List[Any]]:
        query, values, args = self.prep_query()
        placeholder_values = [values[arg] for arg in args]
        return query, placeholder_values

    def prepare(self) -> Tuple[str, Callable[..., List[Any]]]:
        query, values, args = self.prep_query(allow_slots=True)
        env = dict()
        func = [
            "def generate_args(**kwargs):",
            " return [",
        ]
        for i, arg in enumerate(args):
            if isinstance(arg, Slot):
                func.append(f"  kwargs[{repr(arg.name)}],")
            else:
                env[f"value_{i}"] = values[arg]
                func.append(f"  value_{i},")
        func += [" ]"]
        exec("\n".join(func), env)
        return query, env["generate_args"]  # type: ignore

    def __iter__(self) -> Iterator[Any]:
        sql, args = self.query()
        return iter((sql, *args))

    def join(self, parts: Iterable["Fragment"]) -> "Fragment":
        return Fragment(list(join_parts(parts, infix=self)), {})


class SQLFormatter:
    def __call__(
        self, fmt: str, *args, preserve_formatting=False, **kwargs
    ) -> Fragment:
        if not preserve_formatting:
            fmt = newline_whitespace_re.sub(" ", fmt)
        fmtr = string.Formatter()
        parts: List[Part] = []
        values: Dict[Placeholder, Any] = {}
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
                except KeyError:
                    value = Slot(field_name)
                if isinstance(value, Fragment) or isinstance(value, Slot):
                    parts.append(value)
                else:
                    if field_name not in placeholders:
                        placeholders[field_name] = Placeholder(field_name)
                    part = placeholders[field_name]
                    parts.append(part)
                    values[part] = value
        return Fragment(parts, values)

    @staticmethod
    def value(value: Any):
        placeholder = Placeholder("value")
        return Fragment([placeholder], {placeholder: value})

    @staticmethod
    def escape(value: Any) -> Fragment:
        return lit(escape(value))

    @staticmethod
    def slot(name: str) -> Fragment:
        return Fragment([Slot(name)], {})

    @staticmethod
    def literal(text: str):
        return Fragment([text], {})

    @staticmethod
    def identifier(name: str, prefix: Optional[str] = None):
        if prefix:
            return lit(f"{quote_identifier(prefix)}.{quote_identifier(name)}")
        else:
            return lit(f"{quote_identifier(name)}")

    @staticmethod
    def all(parts: Iterable[Fragment]) -> Fragment:
        return any_all(list(parts), "AND", "TRUE")

    @staticmethod
    def any(parts: Iterable[Fragment]) -> Fragment:
        return any_all(list(parts), "OR", "FALSE")

    @staticmethod
    def list(parts: Iterable[Fragment]) -> Fragment:
        return Fragment(list(join_parts(parts, infix=", ")), {})

    @staticmethod
    def unnest(data: Iterable[Sequence[Any]], types: Iterable[str]) -> Fragment:
        nested = list(nest_for_type(x, t) for x, t in zip(zip(*data), types))
        if not nested:
            nested = list(nest_for_type([], t) for t in types)
        return Fragment(["UNNEST(", sql.list(nested), ")"], {})


sql = SQLFormatter()


json_types = ("JSON", "JSONB")


def is_json_type(typename: str) -> bool:
    return typename.upper() in json_types


def nest_for_type(data: Sequence[Any], typename: str) -> Fragment:
    ph = Placeholder("data")

    if is_json_type(typename):
        # https://github.com/MagicStack/asyncpg/issues/345

        # KLUDGE - this doesn't work for trying to store literal
        # strings when autoconverting; None is treated as SQL NULL
        processed_data = [
            x if x is None or isinstance(x, str) else json.dumps(x) for x in data
        ]
        return Fragment([ph, f"::TEXT[]::{typename}[]"], {ph: processed_data})
    else:
        return Fragment([ph, f"::{typename}[]"], {ph: data})


def lit(text: str):
    return Fragment([text], {})


def any_all(frags: List[Fragment], op: str, base_case: str):
    if not frags:
        return lit(base_case)
    parts = join_parts(frags, prefix="(", infix=f") {op} (", suffix=")")
    return Fragment(list(parts), {})


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


def quote_identifier(name: str):
    quoted = name.replace('"', '""')
    return f'"{quoted}"'
