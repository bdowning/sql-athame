from sql_athame import SqlA, format as Q


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


print(get_orders({}).query())
# ('SELECT * FROM orders WHERE TRUE', [])

print(get_orders({"id": "xyzzy"}).query())
# ('SELECT * FROM orders WHERE TRUE AND id = $1', ['xyzzy'])

print(
    get_orders(
        {"eventId": "plugh", "from": "2019-05-01", "until": "2019-08-26"}
    ).query()
)
# ('SELECT * FROM orders WHERE TRUE AND event_id = $1 AND start_time >= $2 AND start_time < $3', ['plugh', '2019-05-01', '2019-08-26'])


superquery = Q(
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
)
print(superquery.query())
# ("""
#     SELECT *
#       FROM (SELECT * FROM orders WHERE TRUE AND id = $1) sq
#       JOIN other_table ot ON (ot.id = sq.id)
#       WHERE ot.foo = $2
#       LIMIT $3
#     """, ['xyzzy', 'bork', 50])
