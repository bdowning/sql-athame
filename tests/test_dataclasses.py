import dataclasses
from assertpy import assert_that

from sql_athame import Q
from sql_athame.dataclasses import ModelBase, model_field


def test_modelclass():
    @dataclasses.dataclass
    class Test(ModelBase):
        class Meta:
            table_name = "table"

        foo: int = model_field(sql="INTEGER NOT NULL")
        bar: str = model_field(default="hi", sql="TEXT NOT NULL")

    t = Test(42)

    assert_that(Q(", ").join(t.field_names()).query()).is_equal_to(("\"foo\", \"bar\"", []))
    assert_that(
        Q(", ").join(t.field_names(prefix="test", exclude=("foo"))).query()
    ).is_equal_to(("\"test\".\"bar\"", []))

    assert_that(Q(", ").join(Test.field_names()).query()).is_equal_to(("\"foo\", \"bar\"", []))

    assert_that(list(Test.create_table_query())).is_equal_to(['CREATE TABLE IF NOT EXISTS "table" ("foo" INTEGER NOT NULL, "bar" TEXT NOT NULL)'])
    assert_that(
        Q(
            "INSERT INTO table ({}) VALUES ({})",
            Q(",").join(t.field_names()),
            Q(",").join(t.field_values()),
        ).query()
    ).is_equal_to(("INSERT INTO table (\"foo\",\"bar\") VALUES ($1,$2)", [42, "hi"]))
