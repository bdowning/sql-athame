from assertpy import assert_that

from sql_athame import Q


def get_orders(query):
    where = [Q("TRUE")]

    if "id" in query:
        where.append(Q("id = {}", query["id"]))
    if "eventId" in query:
        where.append(Q("event_id = {}", query["eventId"]))
    if "startTime" in query:
        where.append(Q("start_time = {}", query["startTime"]))
    if "from" in query:
        where.append(Q("start_time >= {}", query["from"]))
    if "until" in query:
        where.append(Q("start_time < {}", query["until"]))

    return Q("SELECT * FROM orders WHERE {}", Q(" AND ").join(where))


def test_basic():
    assert_that(get_orders({}).query()).is_equal_to(
        ("SELECT * FROM orders WHERE TRUE", [])
    )
    assert_that(get_orders({"id": "xyzzy"}).query()).is_equal_to(
        ("SELECT * FROM orders WHERE TRUE AND id = $1", ["xyzzy"])
    )
    assert_that(
        get_orders(
            {"eventId": "plugh", "from": "2019-05-01", "until": "2019-08-26"}
        ).query()
    ).is_equal_to(
        (
            "SELECT * FROM orders WHERE TRUE AND event_id = $1 AND start_time >= $2 AND start_time < $3",
            ["plugh", "2019-05-01", "2019-08-26"],
        )
    )

    assert_that(
        Q(
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
        ).query()
    ).is_equal_to(
        (
            """
            SELECT *
              FROM (SELECT * FROM orders WHERE TRUE AND id = $1) sq
              JOIN other_table ot ON (ot.id = sq.id)
              WHERE ot.foo = $2
              LIMIT $3
            """,
            ["xyzzy", "bork", 50],
        )
    )


def test_iter():
    assert_that(list(get_orders({}))).is_equal_to(["SELECT * FROM orders WHERE TRUE"])
    assert_that(list(get_orders({"id": "xyzzy"}))).is_equal_to(
        ["SELECT * FROM orders WHERE TRUE AND id = $1", "xyzzy"]
    )


def test_all_any_list():
    assert_that(list(Q.all([Q("a"), Q("b"), Q("c")]))).is_equal_to(
        ["(a) AND (b) AND (c)"]
    )
    assert_that(list(Q.any([Q("a"), Q("b"), Q("c")]))).is_equal_to(
        ["(a) OR (b) OR (c)"]
    )
    assert_that(list(Q.list([Q("a"), Q("b"), Q("c")]))).is_equal_to(["a, b, c"])


def test_repeated_value_keyword():
    assert_that(Q("SELECT {a}, {b}, {a}", a="a", b="b").query()).is_equal_to(
        ("SELECT $1, $2, $1", ["a", "b"])
    )


def test_repeated_value_nested():
    sq_a = get_orders({"id": "a"})
    sq_b = get_orders({"id": "b"})

    assert_that(
        Q(
            """
            SELECT a.*, b.*
              FROM ({sq_a}) a,
                   ({sq_b}) b
            """,
            sq_a=sq_a,
            sq_b=sq_b,
        ).query()
    ).is_equal_to(
        (
            """
            SELECT a.*, b.*
              FROM (SELECT * FROM orders WHERE TRUE AND id = $1) a,
                   (SELECT * FROM orders WHERE TRUE AND id = $2) b
            """,
            ["a", "b"],
        )
    )

    sq_a = get_orders({"id": "a"})
    sq_b = get_orders({"id": "b"})

    assert_that(
        Q(
            """
            SELECT a.*, b.*
              FROM ({sq_a}) a,
                   ({sq_b}) b
            """,
            sq_a=sq_a,
            sq_b=sq_a,
        ).query()
    ).is_equal_to(
        (
            """
            SELECT a.*, b.*
              FROM (SELECT * FROM orders WHERE TRUE AND id = $1) a,
                   (SELECT * FROM orders WHERE TRUE AND id = $1) b
            """,
            ["a"],
        )
    )
