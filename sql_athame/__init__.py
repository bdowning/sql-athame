import dataclasses
import inspect
import re
import string
from typing import Any, Iterator, List, Tuple, Union, cast


@dataclasses.dataclass
class Placeholder:
    type: str


value_placeholder = Placeholder("value")
Part = Union[str, Placeholder, "SqlA"]
FlatPart = Union[str, Placeholder]


def auto_numbered(field_name):
    return not re.match(r"[A-Za-z0-9_]", field_name)


@dataclasses.dataclass
class SqlA:
    parts: List[Part]
    values: List[Any]

    def flatten_into(self, parts: List[FlatPart], values: List[Any]):
        next_value = 0
        for part in self.parts:
            if isinstance(part, SqlA):
                part.flatten_into(parts, values)
            elif part is value_placeholder:
                value = self.values[next_value]
                next_value += 1
                parts.append(part)
                values.append(value)
            else:
                parts.append(part)

    def flatten(self) -> "SqlA":
        parts: List[FlatPart] = []
        values: List[Any] = []
        self.flatten_into(parts, values)
        return SqlA(cast(List[Part], parts), values)

    def query(self) -> Tuple[str, List[Any]]:
        parts: List[FlatPart] = []
        values: List[Any] = []
        self.flatten_into(parts, values)
        placeholder_count = 0
        out_parts: List[str] = []
        for part in parts:
            if part is value_placeholder:
                placeholder_count += 1
                out_parts.append(f"${placeholder_count}")
            else:
                assert isinstance(part, str)
                out_parts.append(part)
        return "".join(out_parts), values

    def join_parts(self, parts: List[Part]) -> Iterator[Part]:
        first = True
        for part in parts:
            if not first:
                yield self
            yield part
            first = False

    def join(self, parts: List[Part]):
        return SqlA(list(self.join_parts(parts)), [])


def format(fmt: str, *args, **kwargs):
    fmtr = string.Formatter()
    parts: List[Part] = []
    values: List[Any] = []
    next_auto_field = 0
    for literal_text, field_name, format_spec, conversion in fmtr.parse(fmt):
        parts.append(literal_text)
        if field_name is not None:
            if auto_numbered(field_name):
                field_name = f"{next_auto_field}{field_name}"
                next_auto_field += 1
            value = fmtr.get_field(field_name, args, kwargs)[0]
            if isinstance(value, SqlA):
                parts.append(value)
            else:
                parts.append(value_placeholder)
                values.append(value)
    return SqlA(parts, values)
