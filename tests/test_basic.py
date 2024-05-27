import uuid

import pytest

from sql_athame import sql


def get_orders(query):
    where = [sql("TRUE")]

    if "id" in query:
        where.append(sql("id = {}", query["id"]))
    if "eventId" in query:
        where.append(sql("event_id = {}", query["eventId"]))
    if "startTime" in query:
        where.append(sql("start_time = {}", query["startTime"]))
    if "from" in query:
        where.append(sql("start_time >= {}", query["from"]))
    if "until" in query:
        where.append(sql("start_time < {}", query["until"]))

    return sql("SELECT * FROM orders WHERE {}", sql(" AND ").join(where))


def test_basic():
    assert get_orders({}).query() == ("SELECT * FROM orders WHERE TRUE", [])
    assert get_orders({"id": "xyzzy"}).query() == (
        "SELECT * FROM orders WHERE TRUE AND id = $1",
        ["xyzzy"],
    )
    assert get_orders(
        {"eventId": "plugh", "from": "2019-05-01", "until": "2019-08-26"}
    ).query() == (
        "SELECT * FROM orders WHERE TRUE AND event_id = $1 AND start_time >= $2 AND start_time < $3",
        ["plugh", "2019-05-01", "2019-08-26"],
    )

    assert sql(
        """
            SELECT *
              FROM ({subquery}) sq
              JOIN other_table ot ON (ot.id = sq.id)
              WHERE ot.foo = {foo}
              LIMIT {limit}
            """,
        subquery=get_orders({"id": "xyzzy"}),
        foo="bork",
        limit=50,
    ).query() == (
        "SELECT *"
        " FROM (SELECT * FROM orders WHERE TRUE AND id = $1) sq"
        " JOIN other_table ot ON (ot.id = sq.id)"
        " WHERE ot.foo = $2"
        " LIMIT $3",
        ["xyzzy", "bork", 50],
    )


def test_iter():
    assert list(get_orders({})) == ["SELECT * FROM orders WHERE TRUE"]
    assert list(get_orders({"id": "xyzzy"})) == [
        "SELECT * FROM orders WHERE TRUE AND id = $1",
        "xyzzy",
    ]


def test_all_any_list():
    assert list(sql.all([sql("a"), sql("b"), sql("c")])) == ["(a) AND (b) AND (c)"]
    assert list(sql.any([sql("a"), sql("b"), sql("c")])) == ["(a) OR (b) OR (c)"]
    assert list(sql.list([sql("a"), sql("b"), sql("c")])) == ["a, b, c"]


def test_all_any_list_star():
    assert list(sql.all(sql("a"), sql("b"), sql("c"))) == ["(a) AND (b) AND (c)"]
    assert list(sql.any(sql("a"), sql("b"), sql("c"))) == ["(a) OR (b) OR (c)"]
    assert list(sql.list(sql("a"), sql("b"), sql("c"))) == ["a, b, c"]


def test_repeated_value_keyword():
    assert sql("SELECT {a}, {b}, {a}", a="a", b="b").query() == (
        "SELECT $1, $2, $1",
        ["a", "b"],
    )


def test_repeated_value_nested():
    sq_a = get_orders({"id": "a"})
    sq_b = get_orders({"id": "b"})

    assert sql(
        """
            SELECT a.*, b.*
              FROM ({sq_a}) a,
                   ({sq_b}) b
            """,
        sq_a=sq_a,
        sq_b=sq_b,
    ).query() == (
        "SELECT a.*, b.*"
        " FROM (SELECT * FROM orders WHERE TRUE AND id = $1) a,"
        " (SELECT * FROM orders WHERE TRUE AND id = $2) b",
        ["a", "b"],
    )

    sq_a = get_orders({"id": "a"})
    sq_b = get_orders({"id": "b"})

    assert sql(
        """
            SELECT a.*, b.*
              FROM ({sq_a}) a,
                   ({sq_b}) b
            """,
        sq_a=sq_a,
        sq_b=sq_a,
    ).query() == (
        "SELECT a.*, b.*"
        " FROM (SELECT * FROM orders WHERE TRUE AND id = $1) a,"
        " (SELECT * FROM orders WHERE TRUE AND id = $1) b",
        ["a"],
    )

    assert sql(
        """
            SELECT a.*, b.*
              FROM ({sq_a}) a,
                   ({sq_b}) b
            """,
        sq_a=sq_a,
        sq_b=sq_a,
    ).query() == (
        "SELECT a.*, b.*"
        " FROM (SELECT * FROM orders WHERE TRUE AND id = $1) a,"
        " (SELECT * FROM orders WHERE TRUE AND id = $1) b",
        ["a"],
    )


def test_any_all():
    assert list(sql.all([])) == ["TRUE"]
    assert list(sql.any([])) == ["FALSE"]

    assert list(sql.all([sql("a")])) == ["(a)"]
    assert list(sql.any([sql("a")])) == ["(a)"]

    assert list(sql.all([sql("a"), sql("b"), sql("c")])) == ["(a) AND (b) AND (c)"]

    assert list(sql.any([sql("a"), sql("b"), sql("c")])) == ["(a) OR (b) OR (c)"]


def test_any_all_star():
    assert list(sql.all()) == ["TRUE"]
    assert list(sql.any()) == ["FALSE"]

    assert list(sql.all(sql("a"))) == ["(a)"]
    assert list(sql.any(sql("a"))) == ["(a)"]

    assert list(sql.all(sql("a"), sql("b"), sql("c"))) == ["(a) AND (b) AND (c)"]

    assert list(sql.any(sql("a"), sql("b"), sql("c"))) == ["(a) OR (b) OR (c)"]


def test_unnest():
    data = [[1, "foo"], [2, "bar"]]
    query = sql.unnest(data, ("INTEGER", "TEXT"))
    assert list(query) == ["UNNEST($1::INTEGER[], $2::TEXT[])", (1, 2), ("foo", "bar")]


def test_unfilled_positional_arg():
    with pytest.raises(ValueError, match="unfilled positional argument"):
        sql("foo {}")


@pytest.mark.parametrize(
    "query",
    [
        sql("SELECT * FROM foo WHERE id = {}", sql.slot("id")),
        sql("SELECT * FROM foo WHERE id = {id}"),
    ],
)
def test_slots(query):
    with pytest.raises(ValueError, match="Unfilled slot: 'id'"):
        query.query()
    with pytest.raises(ValueError, match="Unfilled slot: 'id'"):
        list(query)
    assert list(query.fill(id="foo")) == ["SELECT * FROM foo WHERE id = $1", "foo"]

    assert list(query.fill(id=sql.literal("foo"))) == [
        "SELECT * FROM foo WHERE id = foo"
    ]


def test_slots_same_id_placeholder():
    query = sql("SELECT * FROM foo WHERE start > {id} AND end < {id}")
    assert list(query.fill(id="foo")) == [
        "SELECT * FROM foo WHERE start > $1 AND end < $1",
        "foo",
    ]


def test_slots_compiled():
    query = sql("SELECT * FROM foo WHERE id = {id} AND bar = {}", 42)
    fn = query.compile()
    assert list(fn(id="foo")) == [
        "SELECT * FROM foo WHERE id = $1 AND bar = $2",
        "foo",
        42,
    ]

    assert list(fn(id=sql.literal("foo"))) == [
        "SELECT * FROM foo WHERE id = foo AND bar = $1",
        42,
    ]


def test_slots_compiled_same_id_placeholder():
    query = sql("SELECT * FROM foo WHERE start > {id} AND end < {id}")
    fn = query.compile()
    assert list(fn(id="foo")) == [
        "SELECT * FROM foo WHERE start > $1 AND end < $1",
        "foo",
    ]


def test_prepare():
    query = sql("SELECT * FROM foo WHERE start > {start} AND end < {end}", end="end")
    q, generate_args = query.prepare()
    assert q == "SELECT * FROM foo WHERE start > $1 AND end < $2"
    assert generate_args(start="start") == ["start", "end"]
    with pytest.raises(KeyError):
        generate_args()


def test_preserve_formatting():
    query = sql("SELECT *   \n    FROM foo")
    assert list(query) == ["SELECT * FROM foo"]

    query = sql("SELECT *   \n    FROM foo", preserve_formatting=True)
    assert list(query) == ["SELECT *   \n    FROM foo"]

    # leading and trailing whitespace is still stripped
    query = sql("    SELECT *   \n    FROM foo  ", preserve_formatting=True)
    assert list(query) == ["SELECT *   \n    FROM foo"]

    # spacing with no newlines is preserved always
    query = sql("SELECT 'string   with   spaces'")
    assert list(query) == ["SELECT 'string   with   spaces'"]


def test_escape():
    query = sql("SELECT {}", sql.escape("funky\nstring"))
    assert list(query) == ["SELECT E'funky\\nstring'"]

    query = sql("SELECT {}", sql.escape(4))
    assert list(query) == ["SELECT 4"]

    query = sql("SELECT {}", sql.escape(4.0))
    assert list(query) == ["SELECT 4.0"]

    with pytest.raises(ValueError, match="Can't escape NaN float"):
        query = sql("SELECT {}", sql.escape(float("nan")))

    with pytest.raises(ValueError, match="Can't escape infinite float"):
        query = sql("SELECT {}", sql.escape(1e1234567))

    with pytest.raises(ValueError, match="Can't escape infinite float"):
        query = sql("SELECT {}", sql.escape(-1e1234567))

    query = sql(
        "SELECT {}", sql.escape(uuid.UUID("66c41d78-5ebc-4f96-a05b-85c92a15a9a1"))
    )
    assert list(query) == ["SELECT '66c41d78-5ebc-4f96-a05b-85c92a15a9a1'::UUID"]

    query = sql("SELECT {}", sql.escape([]))
    assert list(query) == ["SELECT ARRAY[]"]

    query = sql("SELECT {}", sql.escape([42, 3]))
    assert list(query) == ["SELECT ARRAY[42, 3]"]

    query = sql("SELECT {}", sql.escape(["str", "funky\nstring"]))
    assert list(query) == ["SELECT ARRAY[E'str', E'funky\\nstring']"]

    query = sql("SELECT {}", sql.escape(None))
    assert list(query) == ["SELECT NULL"]

    with pytest.raises(TypeError):
        query = sql("SELECT {}", sql.escape({}))
