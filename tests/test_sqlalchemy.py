import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import bindparam
from sqlalchemy.types import Float

from sql_athame import sql


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        (42, (42, "Integer()")),
        (bindparam("x", 42, type_=Float()), (42, "Float()")),
    ],
)
def test_positional(arg, expected):
    q = sql("FOO {}", arg)
    stmt = q.sqlalchemy_text()
    assert [(x.value, repr(x.type)) for x in stmt._bindparams.values()] == [expected]
    compiled = stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    assert str(compiled) == "FOO (42)"


@pytest.mark.parametrize(
    ("arg", "expected"),
    [
        (42, (42, "Integer()")),
        (bindparam("x", 42, type_=Float()), (42, "Float()")),
    ],
)
def test_keyword(arg, expected):
    q = sql("FOO {kw}", kw=arg)
    stmt = q.sqlalchemy_text()
    assert [(x.value, repr(x.type)) for x in stmt._bindparams.values()] == [expected]
    compiled = stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    assert str(compiled) == "FOO (42)"


def test_slot():
    q = sql("FOO {slot}")
    stmt = q.sqlalchemy_text()
    assert [(x.key, x.value, repr(x.type)) for x in stmt._bindparams.values()] == [
        ("slot", None, "NullType()")
    ]
    compiled = stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    assert str(compiled) == "FOO (NULL)"
    stmt = stmt.bindparams(slot=42)
    assert [(x.key, x.value, repr(x.type)) for x in stmt._bindparams.values()] == [
        ("slot", 42, "Integer()")
    ]
    compiled = stmt.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
    assert str(compiled) == "FOO (42)"
