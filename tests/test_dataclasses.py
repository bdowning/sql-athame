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


def test_upsert_insert_only():
    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        foo: int
        bar: str
        created_at: str

    t = Test(1, 42, "str", "2023-01-01")

    # Test with insert_only parameter - created_at should be excluded from UPDATE
    assert list(t.upsert_sql(t.insert_sql(), insert_only={"created_at"})) == [
        'INSERT INTO "table" ("id", "foo", "bar", "created_at") VALUES ($1, $2, $3, $4) '
        'ON CONFLICT ("id") DO UPDATE SET "foo"=EXCLUDED."foo", "bar"=EXCLUDED."bar"',
        1,
        42,
        "str",
        "2023-01-01",
    ]

    # Test with both exclude and insert_only-style exclude
    assert list(t.upsert_sql(t.insert_sql(), insert_only={"bar", "created_at"})) == [
        'INSERT INTO "table" ("id", "foo", "bar", "created_at") VALUES ($1, $2, $3, $4) '
        'ON CONFLICT ("id") DO UPDATE SET "foo"=EXCLUDED."foo"',
        1,
        42,
        "str",
        "2023-01-01",
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


def test_insert_only_column_info():
    """Test that ColumnInfo(insert_only=True) works correctly."""

    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        name: str
        created_at: Annotated[str, ColumnInfo(insert_only=True)]
        updated_at: str

    # Test that insert_only fields are detected
    insert_only_fields = Test.insert_only_field_names()
    assert insert_only_fields == {"created_at"}

    # Test that upsert SQL automatically excludes insert_only fields from UPDATE
    test_instance = Test(1, "Alice", "2023-01-01", "2023-01-02")
    insert_sql = test_instance.insert_sql()

    # upsert_sql should automatically handle insert_only fields
    upsert_sql = Test.upsert_sql(insert_sql)

    # The upsert should include created_at in INSERT but exclude it from UPDATE
    upsert_query = upsert_sql.query()[0]
    assert "created_at" in upsert_query  # Should be in INSERT part
    # The UPDATE SET clause should only include name and updated_at
    assert '"name"=EXCLUDED."name"' in upsert_query
    assert '"updated_at"=EXCLUDED."updated_at"' in upsert_query
    assert '"created_at"=EXCLUDED."created_at"' not in upsert_query


def test_insert_only_automatic_handling():
    """Test that the upsert() method automatically handles insert_only fields."""

    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        name: str
        created_at: Annotated[str, ColumnInfo(insert_only=True)]
        updated_at: str

    # Test automatic handling by checking the generated SQL from the upsert method
    test_instance = Test(1, "Alice", "2023-01-01", "2023-01-02")

    # Get the SQL that would be generated for upsert operation
    # We simulate what happens inside the upsert method
    all_insert_only = test_instance.insert_only_field_names()
    insert_sql = test_instance.insert_sql()
    upsert_sql = Test.upsert_sql(insert_sql, insert_only=all_insert_only)

    upsert_query = upsert_sql.query()[0]

    # created_at should be excluded from UPDATE clause automatically
    assert '"name"=EXCLUDED."name"' in upsert_query
    assert '"updated_at"=EXCLUDED."updated_at"' in upsert_query
    assert '"created_at"=EXCLUDED."created_at"' not in upsert_query


def test_insert_only_merge_with_manual():
    """Test that ColumnInfo insert_only merges with manual insert_only parameter."""

    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        name: str
        created_at: Annotated[str, ColumnInfo(insert_only=True)]  # Auto insert-only
        updated_at: str
        version: int

    # Verify auto-detected fields
    assert Test.insert_only_field_names() == {"created_at"}

    # Test combining auto-detected with manual exclude
    test_instance = Test(1, "Alice", "2023-01-01", "2023-01-02", 1)

    # Manual exclude fields should combine with automatic insert_only fields
    manual_exclude = {"version"}

    insert_sql = test_instance.insert_sql()
    upsert_sql = Test.upsert_sql(insert_sql, insert_only=manual_exclude)
    upsert_query = upsert_sql.query()[0]

    # Both created_at (auto) and version (manual) should be excluded from UPDATE
    assert '"name"=EXCLUDED."name"' in upsert_query
    assert '"updated_at"=EXCLUDED."updated_at"' in upsert_query
    assert '"created_at"=EXCLUDED."created_at"' not in upsert_query
    assert '"version"=EXCLUDED."version"' not in upsert_query


def test_column_info_merge_insert_only():
    """Test that ColumnInfo.merge handles insert_only properly."""

    base_info = ColumnInfo(type="TEXT")
    insert_only_info = ColumnInfo(insert_only=True)

    # Test merging - insert_only should be preserved
    merged = ColumnInfo.merge(base_info, insert_only_info)
    assert merged.insert_only is True
    assert merged.type == "TEXT"

    # Test merging the other way
    merged2 = ColumnInfo.merge(insert_only_info, base_info)
    assert merged2.insert_only is True  # Should remain True
    assert merged2.type == "TEXT"

    # Test with both having insert_only set
    both_false = ColumnInfo(type="INTEGER", insert_only=False)
    merged3 = ColumnInfo.merge(both_false, insert_only_info)
    assert merged3.insert_only is True  # True should take precedence

    # Test with None (default)
    none_info = ColumnInfo(type="BIGINT")
    merged4 = ColumnInfo.merge(none_info, insert_only_info)
    assert merged4.insert_only is True


def test_upsert_sql_honors_insert_only_automatically():
    """Test that upsert_sql automatically excludes fields marked with ColumnInfo(insert_only=True)."""

    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        name: str
        created_at: Annotated[str, ColumnInfo(insert_only=True)]
        updated_at: str

    test_instance = Test(1, "Alice", "2023-01-01", "2023-01-02")
    insert_sql = test_instance.insert_sql()

    # Call upsert_sql WITHOUT manually passing insert_only fields
    upsert_sql = Test.upsert_sql(insert_sql)
    upsert_query = upsert_sql.query()[0]

    # The upsert should include created_at in INSERT but exclude it from UPDATE
    assert "created_at" in upsert_query  # Should be in INSERT part
    # The UPDATE SET clause should only include name and updated_at
    assert '"name"=EXCLUDED."name"' in upsert_query
    assert '"updated_at"=EXCLUDED."updated_at"' in upsert_query
    assert '"created_at"=EXCLUDED."created_at"' not in upsert_query

    # Test that manual exclude still works in combination
    upsert_sql_with_exclude = Test.upsert_sql(insert_sql, insert_only={"updated_at"})
    upsert_query_with_exclude = upsert_sql_with_exclude.query()[0]

    # Now both created_at (auto) and updated_at (manual) should be excluded from UPDATE
    assert '"name"=EXCLUDED."name"' in upsert_query_with_exclude
    assert '"updated_at"=EXCLUDED."updated_at"' not in upsert_query_with_exclude
    assert '"created_at"=EXCLUDED."created_at"' not in upsert_query_with_exclude


def test_force_update_functionality():
    """Test that force_update parameter overrides insert_only settings."""

    @dataclass
    class Test(ModelBase, table_name="table", primary_key="id"):
        id: int
        name: str
        created_at: Annotated[str, ColumnInfo(insert_only=True)]
        updated_at: str

    test_instance = Test(1, "Alice", "2023-01-01", "2023-01-02")
    insert_sql = test_instance.insert_sql()

    # Test normal behavior - created_at should be excluded from UPDATE
    normal_upsert = Test.upsert_sql(insert_sql)
    normal_query = normal_upsert.query()[0]
    assert '"name"=EXCLUDED."name"' in normal_query
    assert '"updated_at"=EXCLUDED."updated_at"' in normal_query
    assert '"created_at"=EXCLUDED."created_at"' not in normal_query

    # Test force_update - created_at should be included in UPDATE despite insert_only=True
    force_upsert = Test.upsert_sql(insert_sql, force_update={"created_at"})
    force_query = force_upsert.query()[0]
    assert '"name"=EXCLUDED."name"' in force_query
    assert '"updated_at"=EXCLUDED."updated_at"' in force_query
    assert '"created_at"=EXCLUDED."created_at"' in force_query

    # Test force_update with manual insert_only
    manual_upsert = Test.upsert_sql(
        insert_sql, insert_only={"updated_at"}, force_update={"created_at"}
    )
    manual_query = manual_upsert.query()[0]
    assert '"name"=EXCLUDED."name"' in manual_query
    assert '"updated_at"=EXCLUDED."updated_at"' not in manual_query  # Excluded manually
    assert '"created_at"=EXCLUDED."created_at"' in manual_query  # Force updated

    # Test partial force_update - only override specific fields
    partial_upsert = Test.upsert_sql(
        insert_sql, insert_only={"name"}, force_update={"created_at"}
    )
    partial_query = partial_upsert.query()[0]
    assert '"name"=EXCLUDED."name"' not in partial_query  # Excluded manually
    assert '"updated_at"=EXCLUDED."updated_at"' in partial_query
    assert '"created_at"=EXCLUDED."created_at"' in partial_query  # Force updated


def test_primary_key_only_table_upsert():
    """Test that upsert works correctly for tables with only primary key columns."""

    @dataclass
    class PrimaryKeyOnly(ModelBase, table_name="pk_only", primary_key="id"):
        id: uuid.UUID

    test_instance = PrimaryKeyOnly(uuid.uuid4())
    insert_sql = test_instance.insert_sql()

    # Test that upsert generates valid SQL with DO NOTHING
    upsert_sql = PrimaryKeyOnly.upsert_sql(insert_sql)
    query, params = upsert_sql.query()

    # Should contain ON CONFLICT DO NOTHING since there are no updatable fields
    assert "ON CONFLICT" in query
    assert "DO NOTHING" in query
    assert "DO UPDATE SET" not in query
    assert len(params) == 1  # Only the ID parameter

    # Test with compound primary key (still no other fields)
    @dataclass
    class CompoundPrimaryKeyOnly(
        ModelBase, table_name="compound_pk_only", primary_key=("id1", "id2")
    ):
        id1: int
        id2: str

    compound_instance = CompoundPrimaryKeyOnly(1, "test")
    compound_insert = compound_instance.insert_sql()
    compound_upsert = CompoundPrimaryKeyOnly.upsert_sql(compound_insert)
    compound_query, compound_params = compound_upsert.query()

    assert "ON CONFLICT" in compound_query
    assert "DO NOTHING" in compound_query
    assert "DO UPDATE SET" not in compound_query
    assert len(compound_params) == 2  # Both ID parameters
