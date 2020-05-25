from dataclasses import dataclass

import asyncpg
import pytest
from assertpy import assert_that

from sql_athame import ModelBase, sql


@pytest.fixture(autouse=True)
@pytest.mark.asyncio
async def conn():
    conn = await asyncpg.connect("postgres://postgres:password@localhost/postgres")
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
@pytest.mark.asyncio
async def tables(conn):
    await conn.execute(*Table1.create_table_sql())
    yield


@pytest.mark.asyncio
async def test_connection(conn):
    assert_that(await conn.fetchval("SELECT 2 + 2")).is_equal_to(4)


@pytest.mark.asyncio
async def test_select(conn, tables):
    assert_that(await conn.fetch("SELECT * FROM table1")).is_length(0)
    await Table1(42, "foo").insert(conn)
    res = await conn.fetchrow("SELECT * FROM table1")
    assert_that(list(res.keys())).is_equal_to(["a", "b"])


@pytest.mark.asyncio
async def test_replace_multiple(conn) -> None:
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
    assert_that(c and u).is_false()
    assert_that(d).is_length(3)
    assert_that(await Test.select(conn)).is_empty()

    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(conn, [], where=sql("a = 1"))
    assert_that(c and u).is_false()
    assert_that(d).is_length(2)
    assert_that([x.id for x in await Test.select(conn)]).is_equal_to([3])

    await conn.execute("DELETE FROM test")
    await Test.insert_multiple(conn, data)

    c, u, d = await Test.replace_multiple(
        conn, [Test(1, 5, "apples"), Test(4, 6, "fred")], where=sql("a = 1")
    )
    assert_that(c).is_length(1)
    assert_that(u).is_length(1)
    assert_that(d).is_length(1)
    assert_that(list(sorted(await Test.select(conn)))).is_equal_to(
        [Test(1, 5, "apples"), Test(3, 2, "quux"), Test(4, 6, "fred")]
    )
