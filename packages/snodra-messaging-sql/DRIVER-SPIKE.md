# Driver spike: asyncpg and asyncmy

Recorded 2026-11-26 against `asyncpg==0.32.0`, `asyncmy==0.2.18`, PostgreSQL 17.7 and MySQL 8.4.7,
the images the tests pin. The spike drove both drivers through the operations the add-on needs; the
tests `tests/test_sql.py` and `tests/test_kit.py` pin the behaviour that matters.

## asyncpg 0.32

- **Placeholders** are numbered (`$1`, `$2`, ...). The add-on writes its SQL with `?` and
  `Store._bind` numbers it, exactly as the Go add-on does for `pgx`.
- **A JSON column** takes a Python text through a cast: `CAST($1 AS jsonb)`. Reading it back as
  `CAST(headers AS TEXT)` returns `str`, so the add-on parses the header object itself.
- **`Connection.execute`** returns the command status (`"INSERT 0 1"`, `"UPDATE 2"`, `"SELECT 1"`),
  not a row count; the add-on reads the last number. `fetch`/`fetchrow`/`fetchval` return records
  that convert to dictionaries.
- **Transactions** are explicit: `transaction = conn.transaction(isolation="read_committed")`, then
  `await transaction.start()`, `commit()` or `rollback()`. The default isolation is already READ
  COMMITTED; the add-on sets it explicitly so the contract is visible in the code.
- **`add_listener` is a coroutine** (`await conn.add_listener(channel, callback)`). A notification is
  delivered only when the appending transaction commits; a rollback delivers nothing. The listener
  needs its own connection (the store opens one, never one from its pool) and `remove_listener`
  takes the same callback object.
- **Times** go in and come out as timezone-aware `datetime` values at the column's milliseconds.
- **`FOR UPDATE SKIP LOCKED`** is accepted after `LIMIT ?`; the add-on's claim order is
  `ORDER BY created_at, seq, id LIMIT ? FOR UPDATE SKIP LOCKED`.
- **Pool release** (`pool.release(conn)`) is awaitable.

## asyncmy 0.2.18

- **Placeholders** are `%s` (the driver's format style), not `?`. The add-on's `?` SQL is turned
  into `%s`; no statement has a literal question mark.
- **A JSON column** takes text through `CAST(%s AS JSON)`. Reading it back with `CAST(... AS CHAR)`
  returns the JSON text with the server's own whitespace (`{"x": 1}`); the add-on compares parsed
  values, never the text.
- **Cursors**: `conn.cursor()` returns tuples; `conn.cursor(asyncmy.cursors.DictCursor)` returns
  dictionaries, which the add-on uses for every read.
- **Isolation**: the default is REPEATABLE READ. The pool is created with
  `init_command="SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"`, and every store-owned
  transaction also sends `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` before `START TRANSACTION`.
  Both variants were proved by a visibility read (a row committed after the transaction began was
  seen by its next read). `SELECT @@transaction_isolation` keeps showing the session default and is
  **not** a proof that the next transaction is READ COMMITTED: `SET TRANSACTION` applies to the next
  transaction without changing the variable.
- **`SELECT ... FOR UPDATE SKIP LOCKED`** is accepted, with `LIMIT` before the locking clause:
  `... ORDER BY created_at, seq, id LIMIT %s FOR UPDATE SKIP LOCKED`. The add-on pins the skip in
  `test_a_claim_skips_a_row_another_transaction_holds` on both engines.
- **A duplicate key** (1062) raises `asyncmy.errors.IntegrityError` and leaves the transaction usable,
  so `Processed.record` catches it and returns `False`; PostgreSQL uses `ON CONFLICT (id_hash) DO
  NOTHING` instead because a duplicate would abort the caller's transaction.
- **`GET_LOCK(name, seconds)`** returns 1 when the named lock was taken and 0 on timeout;
  `RELEASE_LOCK(name)` returns 1. These are the schema lock on MySQL.
- **DATETIME** columns return naive `datetime` values; the layout stores UTC, so the add-on attaches
  UTC on read and binds naive UTC on write. `TIMESTAMPTZ` is not available.
- **Pool release** (`pool.release(conn)`) returns a `Future`, so the add-on awaits it when it is
  awaitable; `pool.close()` then `await pool.wait_closed()` closes the pool.
- **Startup**: the official image logs `ready for connections` once during initialization, before the
  server accepts application connections. The tests therefore poll a query after the container's
  port is up instead of trusting the first log line.

## Choices

- `asyncpg` and `asyncmy` are both asyncio-native, so a store call never blocks the event loop. No
  blocking driver (psycopg, PyMySQL) is imported by the add-on or the core.
- The core imports no SQL client; the drivers live only in `snodra-messaging-sql`.
- `asyncmy` is the maintained asyncio fork of aiomysql with the same wire behaviour; aiomysql was not
  selected because it is not maintained for current Python versions.
