import dataclasses
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Any, Protocol, Union

from typing_extensions import TypeAlias


@dataclasses.dataclass(eq=False)
class Placeholder:
    __slots__ = ["name", "value"]
    name: str
    value: Any


@dataclasses.dataclass(frozen=True)
class Slot:
    __slots__ = ["name"]
    name: str


Part: TypeAlias = Union[str, Placeholder, Slot, "Fragment"]
FlatPart: TypeAlias = Union[str, Placeholder, Slot]

Row: TypeAlias = Mapping[str | int, Any]


class AsyncpgFetchable(Protocol):
    async def execute(self, query: str, *args: Any) -> str: ...
    async def fetch(self, query: str, *args: Any) -> list[Row]: ...


class AsyncpgConnection(Protocol):
    async def execute(self, query: str, *args: Any) -> str: ...
    async def fetch(self, query: str, *args: Any) -> list[Row]: ...
    def cursor(
        self, query: str, *args: Any, prefetch: int | None = None
    ) -> AsyncIterator[Row]: ...


class SqlalchemyAsyncConnection(Protocol):
    async def execute(self, statement: Any, parameters: dict | None = None) -> Any: ...
    def stream(self, statement: Any, parameters: dict | None = None) -> Any: ...


AnyFetchable: TypeAlias = Union[AsyncpgFetchable, SqlalchemyAsyncConnection]
AnyConnection: TypeAlias = Union[AsyncpgConnection, SqlalchemyAsyncConnection]


if TYPE_CHECKING:
    from .base import Fragment
