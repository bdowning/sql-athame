# ruff: noqa: UP007

from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, Optional, Union

import pytest

from sql_athame import sql
from sql_athame.dataclasses import ColumnInfo, ModelBase


def test_modelclass():
    @dataclass
    class Test(ModelBase, table_name="table"):
        foo: int
        bar: str = "hi"

    t = Test(42)

    assert sql(", ").join(t.field_names_sql()).query() == ('"foo", "bar"', [])

    assert sql(", ").join(
        t.field_names_sql(prefix="test", exclude=("foo",))
    ).query() == ('"test"."bar"', [])

    assert sql(", ").join(Test.field_names_sql()).query() == ('"foo", "bar"', [])

    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ("foo" INTEGER NOT NULL, "bar" TEXT NOT NULL)'
    ]

    assert list(Test.select_sql()) == ['SELECT "foo", "bar" FROM "table" WHERE TRUE']

    assert list(Test.select_sql(order_by="bar")) == [
        'SELECT "foo", "bar" FROM "table" WHERE TRUE ORDER BY "bar"'
    ]

    assert list(Test.select_sql(order_by=("bar", "foo"))) == [
        'SELECT "foo", "bar" FROM "table" WHERE TRUE ORDER BY "bar", "foo"'
    ]

    assert list(Test.select_sql(for_update=True)) == [
        'SELECT "foo", "bar" FROM "table" WHERE TRUE FOR UPDATE'
    ]

    assert list(Test.select_sql(order_by=("bar", "foo"), for_update=True)) == [
        'SELECT "foo", "bar" FROM "table" WHERE TRUE ORDER BY "bar", "foo" FOR UPDATE'
    ]

    assert list(t.insert_sql()) == [
        'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)',
        42,
        "hi",
    ]

    assert list(Test.insert_multiple_executemany_chunk_sql(1)) == [
        'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)'
    ]

    assert list(Test.insert_multiple_executemany_chunk_sql(3)) == [
        'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2), ($3, $4), ($5, $6)'
    ]

    assert sql(
        "INSERT INTO table ({}) VALUES ({})",
        sql(",").join(t.field_names_sql()),
        sql(",").join(t.field_values_sql()),
    ).query() == ('INSERT INTO table ("foo","bar") VALUES ($1,$2)', [42, "hi"])

    assert list(
        sql(
            "SELECT {fields} FROM {tbl}",
            fields=sql.list(Test.field_names_sql(as_prepended="p_")),
            tbl=Test.table_name_sql(),
        )
    ) == ['SELECT "foo" AS "p_foo", "bar" AS "p_bar" FROM "table"']


def test_modelclass_implicit_types():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="foo"):
        foo: int
        bar: str
        baz: Optional[uuid.UUID]
        quux: Annotated[int, ColumnInfo(constraints="REFERENCES foobar")]
        quuux: Annotated[
            int,
            ColumnInfo(constraints="REFERENCES foobar"),
            ColumnInfo(constraints="BLAH", nullable=True),
        ]
        any: Annotated[Any, ColumnInfo(type="TEXT")]
        any_not_null: Annotated[Any, ColumnInfo(type="TEXT", nullable=False)]
        obj: Annotated[object, ColumnInfo(type="TEXT")]
        obj_not_null: Annotated[object, ColumnInfo(type="TEXT", nullable=False)]
        combined_nullable: Annotated[Union[int, Any], ColumnInfo(type="INTEGER")]
        null_jsonb: Annotated[Optional[dict], ColumnInfo(type="JSONB")]
        not_null_jsonb: Annotated[dict, ColumnInfo(type="JSONB")]

    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ('
        '"foo" INTEGER NOT NULL, '
        '"bar" TEXT NOT NULL, '
        '"baz" UUID, '
        '"quux" INTEGER NOT NULL REFERENCES foobar, '
        '"quuux" INTEGER REFERENCES foobar BLAH, '
        '"any" TEXT, '
        '"any_not_null" TEXT NOT NULL, '
        '"obj" TEXT, '
        '"obj_not_null" TEXT NOT NULL, '
        '"combined_nullable" INTEGER, '
        '"null_jsonb" JSONB, '
        '"not_null_jsonb" JSONB NOT NULL, '
        'PRIMARY KEY ("foo"))'
    ]


@pytest.mark.skipif(sys.version_info < (3, 10), reason="needs python3.10 or greater")
def test_py310_unions():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="foo"):
        foo: int
        bar: str
        baz: uuid.UUID | None
        foo_nullable: int | None
        bar_nullable: str | None

    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ('
        '"foo" INTEGER NOT NULL, '
        '"bar" TEXT NOT NULL, '
        '"baz" UUID, '
        '"foo_nullable" INTEGER, '
        '"bar_nullable" TEXT, '
        'PRIMARY KEY ("foo"))'
    ]


def test_modelclass_missing_type():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="foo"):
        foo: dict

    with pytest.raises(ValueError, match="Missing SQL type for column 'foo'"):
        Test.create_table_sql()


def test_upsert():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        foo: int
        bar: str

    t = Test(1, 42, "str")

    assert list(t.upsert_sql(t.insert_sql())) == [
        'INSERT INTO "table" ("id", "foo", "bar") VALUES ($1, $2, $3) '
        'ON CONFLICT ("id") DO UPDATE SET "foo"=EXCLUDED."foo", "bar"=EXCLUDED."bar"',
        1,
        42,
        "str",
    ]


def test_serial():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: Annotated[int, ColumnInfo(type="SERIAL")]
        foo: int
        bar: str

    assert Test.column_info()["id"].type == "INTEGER"
    assert Test.column_info()["id"].create_type == "SERIAL"
    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ('
        '"id" SERIAL NOT NULL, '
        '"foo" INTEGER NOT NULL, '
        '"bar" TEXT NOT NULL, '
        'PRIMARY KEY ("id"))'
    ]

    query = Test.create_sql(foo=42, bar="foo")
    assert list(query) == [
        'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)'
        ' RETURNING "id", "foo", "bar"',
        42,
        "foo",
    ]


def test_serde():
    @dataclass
    class Test(ModelBase, table_name="table"):
        foo: Annotated[
            str,
            ColumnInfo(serialize=lambda x: x.upper(), deserialize=lambda x: x.lower()),
        ]
        bar: str

    assert Test("foo", "bar").field_values() == ["FOO", "bar"]
    assert Test.create_sql(foo="foo", bar="bar").query() == (
        'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) RETURNING "foo", "bar"',
        ["FOO", "bar"],
    )

    assert Test.from_mapping({"foo": "FOO", "bar": "BAR"}) == Test("foo", "BAR")
    # make sure the monkey patching didn't screw things up
    assert Test.from_mapping({"foo": "FOO", "bar": "BAR"}) == Test("foo", "BAR")

    assert Test.from_prepended_mapping(
        {"p_foo": "FOO", "p_bar": "BAR", "foo": "not foo", "other": "other"}, "p_"
    ) == Test("foo", "BAR")
