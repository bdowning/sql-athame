import dataclasses
from typing import TYPE_CHECKING, Any, Union


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

if TYPE_CHECKING:
    from .base import Fragment
