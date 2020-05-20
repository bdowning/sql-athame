import dataclasses
import uuid
from assertpy import assert_that
from typing import Optional

from sql_athame import sql
from sql_athame.dataclasses import ModelBase, model_field


def test_modelclass():
    @dataclasses.dataclass
    class Test(ModelBase):
        class Meta:
            table_name = "table"

        foo: int = model_field(sql="INTEGER NOT NULL")
        bar: str = model_field(default="hi", sql="TEXT NOT NULL")

    t = Test(42)

    assert_that(sql(", ").join(t.field_names_sql()).query()).is_equal_to(
        ('"foo", "bar"', [])
    )
    assert_that(
        sql(", ").join(t.field_names_sql(prefix="test", exclude=("foo"))).query()
    ).is_equal_to(('"test"."bar"', []))

    assert_that(sql(", ").join(Test.field_names_sql()).query()).is_equal_to(
        ('"foo", "bar"', [])
    )

    assert_that(list(Test.create_table_sql())).is_equal_to(
        [
            'CREATE TABLE IF NOT EXISTS "table" ("foo" INTEGER NOT NULL, "bar" TEXT NOT NULL)'
        ]
    )
    assert_that(
        sql(
            "INSERT INTO table ({}) VALUES ({})",
            sql(",").join(t.field_names_sql()),
            sql(",").join(t.field_values_sql()),
        ).query()
    ).is_equal_to(('INSERT INTO table ("foo","bar") VALUES ($1,$2)', [42, "hi"]))


def test_modelclass_implicit_types():
    @dataclasses.dataclass
    class Test(ModelBase):
        class Meta:
            table_name = "table"

        foo: int
        bar: str
        baz: Optional[uuid.UUID]

    assert_that(list(Test.create_table_sql())).is_equal_to(
        [
            'CREATE TABLE IF NOT EXISTS "table" ("foo" INTEGER NOT NULL, "bar" TEXT NOT NULL, "baz" UUID)'
        ]
    )


def test_upsert():
    @dataclasses.dataclass
    class Test(ModelBase):
        class Meta:
            table_name = "table"
            primary_keys = ("id",)

        id: int
        foo: int
        bar: str

    t = Test(1, 42, "str")

    assert_that(list(t.upsert_sql())).is_equal_to(
        [
            'INSERT INTO "table" ("id", "foo", "bar") VALUES ($1, $2, $3) '
            'ON CONFLICT ("id") DO UPDATE SET "foo"=EXCLUDED."foo", "bar"=EXCLUDED."bar"',
            1,
            42,
            "str",
        ]
    )
