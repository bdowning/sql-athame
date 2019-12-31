import dataclasses
import operator
import re
import string
from typing import Any, Dict, Iterator, List, Tuple, Union, cast


@dataclasses.dataclass(eq=False)
class Placeholder:
    name: str

    def __repr__(self):
        return f"Placeholder(id={id(self)}, name={repr(self.name)})"


Part = Union[str, Placeholder, "SqlA"]
FlatPart = Union[str, Placeholder]


def auto_numbered(field_name):
    return not re.match(r"[A-Za-z0-9_]", field_name)


@dataclasses.dataclass
class SqlA:
    parts: List[Part]
    values: Dict[Placeholder, Any]

    def flatten_into(self, parts: List[FlatPart], values: Dict[Placeholder, Any]):
        for part in self.parts:
            if isinstance(part, SqlA):
                part.flatten_into(parts, values)
            elif isinstance(part, Placeholder):
                parts.append(part)
                values[part] = self.values[part]
            else:
                parts.append(part)

    def flatten(self) -> "SqlA":
        parts: List[FlatPart] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(parts, values)
        return SqlA(cast(List[Part], parts), values)

    def query(self) -> Tuple[str, List[Any]]:
        parts: List[FlatPart] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(parts, values)
        placeholder_count = 0
        placeholder_ids: Dict[Placeholder, int] = {}
        out_parts: List[str] = []
        for part in parts:
            if isinstance(part, Placeholder):
                if part not in placeholder_ids:
                    placeholder_count += 1
                    placeholder_ids[part] = placeholder_count
                out_parts.append(f"${placeholder_ids[part]}")
            else:
                assert isinstance(part, str)
                out_parts.append(part)
        placeholder_values = [
            values[part]
            for part, i in sorted(placeholder_ids.items(), key=operator.itemgetter(1))
        ]
        return "".join(out_parts), placeholder_values

    def join_parts(self, parts: List[Part]) -> Iterator[Part]:
        first = True
        for part in parts:
            if not first:
                yield self
            yield part
            first = False

    def join(self, parts: List[Part]):
        return SqlA(list(self.join_parts(parts)), {})


def format(fmt: str, *args, **kwargs):
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
            value = fmtr.get_field(field_name, args, kwargs)[0]
            if isinstance(value, SqlA):
                parts.append(value)
            else:
                if field_name not in placeholders:
                    placeholders[field_name] = Placeholder(field_name)
                part = placeholders[field_name]
                parts.append(part)
                values[part] = value
    return SqlA(parts, values)
