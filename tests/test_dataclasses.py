import uuid
from dataclasses import dataclass, field
from typing import Optional

from sql_athame import sql
from sql_athame.dataclasses import ModelBase, model_field, model_field_metadata


def test_modelclass():
    @dataclass
    class Test(ModelBase, table_name="table"):
        foo: int = model_field(type="INTEGER", constraints="NOT NULL")
        bar: str = model_field(default="hi", type="TEXT", constraints="NOT NULL")

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

    assert sql(
        "INSERT INTO table ({}) VALUES ({})",
        sql(",").join(t.field_names_sql()),
        sql(",").join(t.field_values_sql()),
    ).query() == ('INSERT INTO table ("foo","bar") VALUES ($1,$2)', [42, "hi"])


def test_modelclass_implicit_types():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="foo"):
        foo: int
        bar: str
        baz: Optional[uuid.UUID]

    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ('
        '"foo" INTEGER NOT NULL, '
        '"bar" TEXT NOT NULL, '
        '"baz" UUID, '
        'PRIMARY KEY ("foo"))'
    ]


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


def test_mapping():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        foo: int
        bar: str

    t = Test(1, 2, "foo")
    assert t["id"] == 1
    assert t["foo"] == 2
    assert t["bar"] == "foo"

    assert list(t.keys()) == ["id", "foo", "bar"]

    assert dict(t) == {"id": 1, "foo": 2, "bar": "foo"}
    assert dict(**t) == {"id": 1, "foo": 2, "bar": "foo"}


def test_serial():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int = field(metadata=model_field_metadata(type="SERIAL"))
        foo: int
        bar: str

    assert Test.column_info("id").type == "INTEGER"
    assert Test.column_info("id").create_type == "SERIAL"
    assert list(Test.create_table_sql()) == [
        'CREATE TABLE IF NOT EXISTS "table" ('
        '"id" SERIAL, '
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
