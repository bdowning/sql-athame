import dataclasses
import operator
import re
import string
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union, cast


@dataclasses.dataclass(eq=False)
class Placeholder:
    name: str

    def __repr__(self):
        return f"Placeholder(id={id(self)}, name={repr(self.name)})"


Part = Union[str, Placeholder, "Fragment"]
FlatPart = Union[str, Placeholder]


def auto_numbered(field_name):
    return not re.match(r"[A-Za-z0-9_]", field_name)


@dataclasses.dataclass
class Fragment:
    parts: List[Part]
    values: Dict[Placeholder, Any] = dataclasses.field(default_factory=dict)

    def flatten_into(self, parts: List[FlatPart], values: Dict[Placeholder, Any]):
        for part in self.parts:
            if isinstance(part, Fragment):
                part.flatten_into(parts, values)
            elif isinstance(part, Placeholder):
                parts.append(part)
                values[part] = self.values[part]
            else:
                parts.append(part)

    def flatten(self) -> "Fragment":
        parts: List[FlatPart] = []
        values: Dict[Placeholder, Any] = {}
        self.flatten_into(parts, values)
        return Fragment(cast(List[Part], parts), values)

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

    def __iter__(self) -> Iterator[Any]:
        sql, args = self.query()
        return iter((sql, *args))

    def join(self, parts: List["Fragment"]):
        return Fragment(list(join_fragments(parts, infix=self)), {})


class SQLFormatter:
    def __call__(self, fmt: str, *args, **kwargs) -> Fragment:
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
                if isinstance(value, Fragment):
                    parts.append(value)
                else:
                    if field_name not in placeholders:
                        placeholders[field_name] = Placeholder(field_name)
                    part = placeholders[field_name]
                    parts.append(part)
                    values[part] = value
        return Fragment(parts, values)

    @staticmethod
    def literal(text: str):
        return Fragment(text)

    @staticmethod
    def identifier(name: str, prefix: Optional[str] = None):
        if prefix:
            return Fragment(f"{quote_identifier(prefix)}.{quote_identifier(name)}")
        else:
            return Fragment(f"{quote_identifier(name)}")

    @staticmethod
    def all(frags: Iterable[Fragment]) -> Fragment:
        frags = list(frags)
        if not frags:
            return Fragment("TRUE")
        parts = join_fragments(
            frags, prefix=Fragment("("), infix=Fragment(") AND ("), suffix=Fragment(")")
        )
        return Fragment(list(parts))

    @staticmethod
    def any(frags: Iterable[Fragment]) -> Fragment:
        frags = list(frags)
        if not frags:
            return Fragment("FALSE")
        parts = join_fragments(
            frags, prefix=Fragment("("), infix=Fragment(") OR ("), suffix=Fragment(")")
        )
        return Fragment(list(parts))

    @staticmethod
    def list(frags: Iterable[Fragment]) -> Fragment:
        parts = join_fragments(frags, infix=Fragment(", "))
        return Fragment(list(parts))


sql = SQLFormatter()


def join_fragments(
    parts: List[Fragment],
    infix: Fragment,
    prefix: Optional[Fragment] = None,
    suffix: Optional[Fragment] = None,
) -> Iterator[Fragment]:
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
