import json
import os
from dataclasses import dataclass, field
from typing import Optional

import asyncpg
import pytest

from sql_athame import ModelBase, model_field_metadata, sql


@pytest.fixture(autouse=True)
async def conn():
    port = os.environ.get("PGPORT", 29329)
    conn = await asyncpg.connect(
        f"postgres://postgres:password@localhost:{port}/postgres"
    )
    txn = conn.transaction()
    try:
        await txn.start()
        yield conn
    finally:
        await txn.rollback()
        await conn.close()


@dataclass
class Table1(ModelBase, table_name="table1"):
    a: int
    b: str


@pytest.fixture(autouse=True)
async def tables(conn):
    await conn.execute(*Table1.create_table_sql())
    yield


async def test_connection(conn):
    assert await conn.fetchval("SELECT 2 + 2") == 4


async def test_select(conn, tables):
    assert len(await conn.fetch("SELECT * FROM table1")) == 0
    await Table1(42, "foo").insert(conn)
    res = await conn.fetchrow("SELECT * FROM table1")
    assert list(res.keys()) == ["a", "b"]


async def test_replace_multiple(conn):
    @dataclass(order=True)
    class Test(ModelBase, table_name="test", primary_key="id"):
        id: int
        a: int
        b: str

    await conn.execute(*Test.create_table_sql())

    data = [
        Test(1, 1, "foo"),
        Test(2, 1, "bar"),
        Test(3, 2, "quux"),
    ]
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(conn, [], where=[])
    assert not c and not u
    assert len(d) == 3
    assert await Test.select(conn) == []

    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(conn, [], where=sql("a = 1"))
    assert not c and not u
    assert len(d) == 2
    assert [x.id for x in await Test.select(conn)] == [3]

    await conn.execute("DELETE FROM test")
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(
        conn, [Test(1, 5, "apples"), Test(4, 6, "fred")], where=sql("a = 1")
    )
    assert len(c) == 1
    assert len(u) == 1
    assert len(d) == 1
    assert list(sorted(await Test.select(conn))) == [
        Test(1, 5, "apples"),
        Test(3, 2, "quux"),
        Test(4, 6, "fred"),
    ]


async def test_replace_multiple_reporting_differences(conn):
    @dataclass(order=True)
    class Test(ModelBase, table_name="test", primary_key="id"):
        id: int
        a: int
        b: str

    await conn.execute(*Test.create_table_sql())

    data = [
        Test(1, 1, "foo"),
        Test(2, 1, "bar"),
        Test(3, 2, "quux"),
    ]
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple_reporting_differences(conn, [], where=[])
    assert not c and not u
    assert len(d) == 3
    assert await Test.select(conn) == []

    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple_reporting_differences(
        conn, [], where=sql("a = 1")
    )
    assert not c and not u
    assert len(d) == 2
    assert [x.id for x in await Test.select(conn)] == [3]

    await conn.execute("DELETE FROM test")
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple_reporting_differences(
        conn, [Test(1, 5, "apples"), Test(4, 6, "fred")], where=sql("a = 1")
    )
    assert len(c) == 1
    assert len(u) == 1
    assert u == [(Test(1, 1, "foo"), Test(1, 5, "apples"), ["a", "b"])]
    assert len(d) == 1
    assert list(sorted(await Test.select(conn))) == [
        Test(1, 5, "apples"),
        Test(3, 2, "quux"),
        Test(4, 6, "fred"),
    ]


async def test_replace_multiple_multicolumn_pk(conn):
    @dataclass(order=True)
    class Test(ModelBase, table_name="test", primary_key=("id1", "id2")):
        id1: int
        id2: int
        a: int
        b: str

    await conn.execute(*Test.create_table_sql())

    data = [
        Test(1, 1, 1, "foo"),
        Test(1, 2, 1, "bar"),
        Test(1, 3, 2, "quux"),
    ]
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(
        conn, [Test(1, 1, 5, "apples"), Test(2, 4, 6, "fred")], where=sql("a = 1")
    )
    assert len(c) == 1
    assert len(u) == 1
    assert len(d) == 1
    assert list(sorted(await Test.select(conn))) == [
        Test(1, 1, 5, "apples"),
        Test(1, 3, 2, "quux"),
        Test(2, 4, 6, "fred"),
    ]


async def test_serial(conn):
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int = field(metadata=model_field_metadata(type="SERIAL"))
        foo: int
        bar: str

    await conn.execute(*Test.create_table_sql())
    t = await Test.create(conn, foo=42, bar="bar")
    assert t == Test(1, 42, "bar")
    t = await Test.create(conn, foo=42, bar="bar")
    assert t == Test(2, 42, "bar")

    assert list(await Test.select(conn)) == [Test(1, 42, "bar"), Test(2, 42, "bar")]


async def test_unnest_json(conn):
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int = field(metadata=model_field_metadata(type="SERIAL"))
        json: Optional[list] = field(metadata=model_field_metadata(type="JSONB"))

    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )

    await conn.execute(*Test.create_table_sql())

    rows = [
        Test(1, ["foo"]),
        Test(2, ["foo", "bar"]),
        Test(3, None),
    ]

    await Test.insert_multiple(conn, rows)

    assert list(await Test.select(conn)) == rows
    assert list(
        await conn.fetchrow('SELECT COUNT(*) FROM "table" WHERE json IS NULL')
    ) == [1]


async def test_unnest_empty(conn):
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int = field(metadata=model_field_metadata(type="SERIAL"))

    await conn.execute(*Test.create_table_sql())

    await Test.insert_multiple(conn, [])

    assert list(await Test.select(conn)) == []
