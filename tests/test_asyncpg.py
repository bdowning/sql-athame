from dataclasses import dataclass

import asyncpg
import pytest
from assertpy import assert_that

from sql_athame import ModelBase


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
class Table1(ModelBase):
    class Meta:
        table_name = "table1"

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
