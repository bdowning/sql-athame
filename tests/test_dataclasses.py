import uuid
from dataclasses import dataclass, field
from typing import Optional

from assertpy import assert_that

from sql_athame import sql
from sql_athame.dataclasses import ModelBase, model_field, model_field_metadata


def test_modelclass():
    @dataclass
    class Test(ModelBase, table_name="table"):
        foo: int = model_field(type="INTEGER", constraints="NOT NULL")
        bar: str = model_field(default="hi", type="TEXT", constraints="NOT NULL")

    t = Test(42)

    assert_that(sql(", ").join(t.field_names_sql()).query()).is_equal_to(
        ('"foo", "bar"', [])
    )
    assert_that(
        sql(", ").join(t.field_names_sql(prefix="test", exclude=("foo",))).query()
    ).is_equal_to(('"test"."bar"', []))

    assert_that(sql(", ").join(Test.field_names_sql()).query()).is_equal_to(
        ('"foo", "bar"', [])
    )

    assert_that(list(Test.create_table_sql())).is_equal_to(
        [
            'CREATE TABLE IF NOT EXISTS "table" ("foo" INTEGER NOT NULL, "bar" TEXT NOT NULL)'
        ]
    )
    assert_that(list(Test.select_sql())).is_equal_to(
        ['SELECT "foo", "bar" FROM "table" WHERE TRUE']
    )
    assert_that(list(t.insert_sql())).is_equal_to(
        ['INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)', 42, "hi"]
    )

    assert_that(
        sql(
            "INSERT INTO table ({}) VALUES ({})",
            sql(",").join(t.field_names_sql()),
            sql(",").join(t.field_values_sql()),
        ).query()
    ).is_equal_to(('INSERT INTO table ("foo","bar") VALUES ($1,$2)', [42, "hi"]))


def test_modelclass_implicit_types():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="foo"):
        foo: int
        bar: str
        baz: Optional[uuid.UUID]

    assert_that(list(Test.create_table_sql())).is_equal_to(
        [
            'CREATE TABLE IF NOT EXISTS "table" ('
            '"foo" INTEGER NOT NULL, '
            '"bar" TEXT NOT NULL, '
            '"baz" UUID, '
            'PRIMARY KEY ("foo"))'
        ]
    )


def test_upsert():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        foo: int
        bar: str

    t = Test(1, 42, "str")

    assert_that(list(t.upsert_sql(t.insert_sql()))).is_equal_to(
        [
            'INSERT INTO "table" ("id", "foo", "bar") VALUES ($1, $2, $3) '
            'ON CONFLICT ("id") DO UPDATE SET "foo"=EXCLUDED."foo", "bar"=EXCLUDED."bar"',
            1,
            42,
            "str",
        ]
    )


def test_mapping():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        foo: int
        bar: str

    t = Test(1, 2, "foo")
    assert_that(t["id"]).is_equal_to(1)
    assert_that(t["foo"]).is_equal_to(2)
    assert_that(t["bar"]).is_equal_to("foo")

    assert_that(list(t.keys())).is_equal_to(["id", "foo", "bar"])

    assert_that(dict(t)).is_equal_to({"id": 1, "foo": 2, "bar": "foo"})
    assert_that(dict(**t)).is_equal_to({"id": 1, "foo": 2, "bar": "foo"})


def test_serial():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int = field(metadata=model_field_metadata(type="SERIAL"))
        foo: int
        bar: str

    assert_that(Test.column_info("id").type).is_equal_to("INTEGER")
    assert_that(Test.column_info("id").create_type).is_equal_to("SERIAL")
    assert_that(list(Test.create_table_sql())).is_equal_to(
        [
            'CREATE TABLE IF NOT EXISTS "table" ('
            '"id" SERIAL, '
            '"foo" INTEGER NOT NULL, '
            '"bar" TEXT NOT NULL, '
            'PRIMARY KEY ("id"))'
        ]
    )

    query = Test.create_sql(foo=42, bar="foo")
    assert_that(list(query)).is_equal_to(
        [
            'INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)'
            ' RETURNING "id", "foo", "bar"',
            42,
            "foo",
        ]
    )
