# sql-athame

Python tool for slicing and dicing SQL.  Its intended target is
Postgres with _asyncpg_.

## API Reference

```python
from sql_athame import sql
```

### sql(fmt: str, \*args, \*\*kwargs) -> Fragment

Creates a SQL `Fragment` from the `fmt` string.  The `fmt` string
contains literal SQL and may contain positional references, marked by
`{}`, and named references, marked by `{name}`.  Positional references
_must_ have a matching argument in `*args`.  Named references _may_
have a matching argument in `**kwargs`; if a named reference is not
fullfilled by `**kwargs` it remains as a named _slot_ to be filled
later.

If a referenced argument is a `Fragment`, it is substituted into the
SQL along with all of its embedded placeholders if any.  Otherwise, it
is treated as a placeholder value and substituted in place as a
placeholder.

### Fragment.query(self) -> Tuple[str, List[Any]]

Renders a SQL `Fragment` into a query string and list of placeholder
parameters.

```python
>>> q = sql("SELECT * FROM tbl WHERE qty > {qty}", qty=10)
>>> q.query()
('SELECT * FROM tbl WHERE qty > $1', [10])
```

If there are any unfilled _slots_ `ValueError` will be raised.

```python
>>> q = sql("SELECT * FROM tbl WHERE qty > {qty}")
>>> q.query()
ValueError: Unfilled slot: 'qty'
>>> q.fill(qty=10).query()
('SELECT * FROM tbl WHERE qty > $1', [10])
```

### Fragment.\_\_iter\_\_(self) -> Iterator[Any]:

A `Fragment` is an iterable which will return the query string
followed by the placeholder parameters as returned by
`Fragment.query(self)`.  This matches the `(query, *args)` argument
pattern of the _asyncpg_ API:

```python
q = sql("SELECT * FROM tbl WHERE qty > {}", 10)
await conn.fetch(*q)
```

### sql.list(parts: Iterable[Fragment]) -> Fragment:

Creates a SQL `Fragment` joining the fragments in `parts` together
with commas.

```python
>>> cols = [sql("a"), sql("b"), sql("c")]
>>> list(sql("SELECT {cols} FROM tbl", cols=sql.list(cols)))
['SELECT a, b, c FROM tbl']
```

### sql.all(parts: Iterable[Fragment]) -> Fragment:

Creates a SQL `Fragment` joining the fragments in `parts` together
with `AND`.  If `parts` is empty, returns `TRUE`.

```python
>>> where = [sql("a = {}", 42), sql("x <> {}", "foo")]
>>> list(sql("SELECT * FROM tbl WHERE {}", sql.all(where)))
['SELECT * FROM tbl WHERE (a = $1) AND (x <> $2)', 42, 'foo']
>>> list(sql("SELECT * FROM tbl WHERE {}", sql.all([])))
['SELECT * FROM tbl WHERE TRUE']
```

### sql.any(parts: Iterable[Fragment]) -> Fragment:

Creates a SQL `Fragment` joining the fragments in `parts` together
with `OR`.  If `parts` is empty, returns `FALSE`.

```python
>>> where = [sql("a = {}", 42), sql("x <> {}", "foo")]
>>> list(sql("SELECT * FROM tbl WHERE {}", sql.any(where)))
['SELECT * FROM tbl WHERE (a = $1) OR (x <> $2)', 42, 'foo']
>>> list(sql("SELECT * FROM tbl WHERE {}", sql.any([])))
['SELECT * FROM tbl WHERE FALSE']
```

### Fragment.join(self, parts: Iterable[Fragment]) -> Fragment:

Creates a SQL `Fragment` by joining the fragments in `parts` together
with `self`.

```python
>>> clauses = [sql("WHEN {} THEN {}", a, b) for a, b in ((sql("a"), 1), (sql("b"), 2))]
>>> case = sql("CASE {clauses} END", clauses=sql(" ").join(clauses))
>>> list(case)
['CASE WHEN a THEN $1 WHEN b THEN $2 END', 1, 2]
```

### sql.literal(text: str) -> Fragment

Creates a SQL `Fragment` with the literal SQL `text`.  No substitution
of any kind is performed.  **Be very careful of SQL injection.**

### sql.identifier(name: str, prefix: Optional[str] = None) -> Fragment

Creates a SQL `Fragment` with a quoted identifier name, optionally
with a dotted prefix.

```python
>>> list(sql("SELECT {a} FROM tbl", a=sql.identifier("a", prefix="tbl")))
['SELECT "tbl"."a" FROM tbl']
```

### sql.value(value: Any) -> Fragment

Creates a SQL `Fragment` with a single placeholder to `value`.
Equivalent to:

```python
sql("{}", value)
```

### sql.escape(value: Any) -> Fragment

Creates a SQL `Fragment` with `value` escaped and embedded into the
SQL.  Types currently supported are strings, floats, ints, UUIDs, and
sequences of the above.

```python
>>> list(sql("SELECT * FROM tbl WHERE qty = ANY({})", sql.escape([1, 3, 5])))
['SELECT * FROM tbl WHERE qty = ANY(ARRAY[1, 3, 5])']
```

Compare to with a placeholder:

```python
>>> list(sql("SELECT * FROM tbl WHERE qty = ANY({})", [1, 3, 5]))
['SELECT * FROM tbl WHERE qty = ANY($1)', [1, 3, 5]]
```

"Burning" an invariant value into the query can potentially help the
query optimizer.

### sql.slot(name: str) -> Fragment

Creates a SQL `Fragment` with a single empty _slot_ named `name`.
Equivalent to:

```python
sql("{name}")
```

### Fragment.fill(self, \*\*kwargs) -> Fragment

Creates a SQL `Fragment` by filling any empty _slots_ in `self` with
`kwargs`.  Similar to `sql` subtitution, if a value is a `Fragment` it
is substituted in-place, otherwise it is substituted as a placeholder.

### Fragment.compile(self) -> Callable[..., Fragment]

Creates a function that when called with `**kwargs` will create a SQL
`Fragment` equivalent to calling `self.fill(**kwargs)`.  This is
optimized to do as much work as possible up front and can be
considerably faster if repeated often.

### Fragment.prepare(self) -> Tuple[str, Callable[..., List[Any]]]

Renders `self` into a SQL query string; returns that string and a
function that when called with `**kwargs` containing the unfilled
slots of `self` will return a list containing the placeholder values
for `self` as filled with `**kwargs`.

```python
>>> query, query_args = sql("UPDATE tbl SET foo={foo}, bar={bar} WHERE baz < {baz}", baz=10).prepare()
>>> query
'UPDATE tbl SET foo=$1, bar=$2 WHERE baz < $3'
>>> query_args(foo=1, bar=2)
[1, 2, 10]
>>> query_args(bar=42, foo=3)
[3, 42, 10]
```

As the name implies this is intended to be used in prepared
statements:

```python
query, query_args = sql("UPDATE tbl SET foo={foo}, bar={bar} WHERE baz < {baz}", baz=10).prepare()
stmt = await conn.prepare(query)
await stmt.execute(*query_args(foo=1, bar=2))
await stmt.execute(*query_args(bar=42, foo=3))
```

## Example

```python
from sql_athame import sql


def get_orders(query):
    where = []

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

    return sql("SELECT * FROM orders WHERE {}", sql.all(where))


print(get_orders({}).query())
# ('SELECT * FROM orders WHERE TRUE', [])

print(list(get_orders({})))
# ['SELECT * FROM orders WHERE TRUE']

print(get_orders({"id": "xyzzy"}).query())
# ('SELECT * FROM orders WHERE TRUE AND id = $1', ['xyzzy'])

print(list(get_orders({"id": "xyzzy"})))
# ['SELECT * FROM orders WHERE TRUE AND id = $1', 'xyzzy']

print(
    *get_orders(
        {"eventId": "plugh", "from": "2019-05-01", "until": "2019-08-26"}
    )
)
# SELECT * FROM orders WHERE TRUE AND event_id = $1 AND start_time >= $2 AND start_time < $3 ['plugh', '2019-05-01', '2019-08-26']


superquery = sql(
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
```

## License

MIT.

---
Copyright (c) 2019, 2020 Brian Downing
