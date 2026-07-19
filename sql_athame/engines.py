from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, KeysView, Mapping
from typing import TYPE_CHECKING, Any

from .types import Row

try:
    from sqlalchemy.engine import Connection as SqlalchemyConnection

    def is_sqlalchemy_sync_connection(conn: Any) -> bool:
        return isinstance(conn, SqlalchemyConnection)

except ImportError:

    def is_sqlalchemy_sync_connection(conn: Any) -> bool:
        return False


try:
    from sqlalchemy.ext.asyncio import AsyncConnection

    def is_sqlalchemy_async_connection(conn: Any) -> bool:
        return isinstance(conn, AsyncConnection)

except ImportError:

    def is_sqlalchemy_async_connection(conn: Any) -> bool:
        return False


try:
    from asyncpg import Connection, Pool

    def is_asyncpg_fetchable(conn: Any) -> bool:
        return isinstance(conn, (Connection, Pool))

    def is_asyncpg_connection(conn: Any) -> bool:
        return isinstance(conn, Connection)

except ImportError:

    def is_asyncpg_fetchable(conn: Any) -> bool:
        return False

    def is_asyncpg_connection(conn: Any) -> bool:
        return False


class SqlalchemyRowWrapper(Mapping[str | int, Any]):
    __slots__ = ("_row",)

    def __init__(self, row):
        self._row = row

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, str):
            return self._row._mapping[key]
        return self._row[key]

    def __getattr__(self, key: str) -> Any:
        return getattr(self._row, key)

    def keys(self) -> KeysView[str | int]:
        return self._row._mapping.keys()

    def __iter__(self) -> Iterator[Any]:
        return iter(self._row)

    def __len__(self) -> int:
        return len(self._row)


async def async_execute(conn: Any, frag: Fragment) -> str:
    if is_asyncpg_fetchable(conn):
        return await conn.execute(*frag)
    elif is_sqlalchemy_async_connection(conn):
        result = await conn.execute(frag.sqlalchemy_text())
        return f"ROWCOUNT {result.rowcount}"
    raise TypeError(f"Unknown connection type: {type(conn)}")


async def async_fetch(conn: Any, frag: Fragment) -> list[Row]:
    if is_asyncpg_fetchable(conn):
        return await conn.fetch(*frag)
    elif is_sqlalchemy_async_connection(conn):
        result = await conn.execute(frag.sqlalchemy_text())
        return [SqlalchemyRowWrapper(r) for r in result.fetchall()]
    raise TypeError(f"Unknown connection type: {type(conn)}")


async def async_cursor(
    conn: Any, frag: Fragment, *, prefetch: int = 1000
) -> AsyncIterator[Row]:
    if is_asyncpg_connection(conn):
        async for row in conn.cursor(*frag, prefetch=prefetch):
            yield row
    elif is_sqlalchemy_async_connection(conn):
        async with conn.stream(
            frag.sqlalchemy_text().execution_options(fetch_size=prefetch)
        ) as result:
            async for row in result:
                yield SqlalchemyRowWrapper(row)
    else:
        raise TypeError(f"Unknown connection type: {type(conn)}")


if TYPE_CHECKING:
    from .base import Fragment
