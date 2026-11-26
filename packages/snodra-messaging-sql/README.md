# snodra-messaging-sql

The PostgreSQL and MySQL message store for the Snodra messaging suite: the outbox, the processed-event
markers and the rest of the message-store ports in the service's own database, on the shared SQL layout
version 1 that every Snodra library reads and writes.

## Install

```sh
pip install snodra-messaging-sql
```

The distribution needs Python 3.12 or later and depends on `snodra-messaging`, `asyncpg` (PostgreSQL) and
`asyncmy` (MySQL). The core library never imports a SQL client; this add-on is where they live.

## Use

```python
import asyncio

import snodra
from snodra_sql import SqlConfig, SqlStore


async def main() -> None:
    store = SqlStore(SqlConfig.from_env())
    try:
        settings = snodra.Settings.from_env(service="orders-service")
        # The event store reads what the relay publishes; the message store holds the outbox and markers.
        app = snodra.App(settings, store=store, event_store=...)
        await app.run()
    finally:
        await store.close()


asyncio.run(main())
```

`SqlConfig.from_env()` reads `SNODRA_STORE_SQL_URL` (required), `SNODRA_STORE_SQL_DRIVER`,
`SNODRA_STORE_SQL_NOTIFY` and `SNODRA_STORE_SQL_PURGE_INTERVAL_MS`. The URL names the database:
`postgres://` or `postgresql://` for PostgreSQL, `mysql://` for MySQL. The driver is read from the scheme when
the variable is empty.

The store implements the level-2 ports: the outbox and the processed-event markers. The dead-letter,
processor-control, external-call and saga ports are part of the interface frozen at level 2 and report
`snodra.MessageStoreUnsupported` for now; the official add-ons implement every port by level 4.

### The transaction is the service's

`async with store.transaction():` begins a transaction, binds it to the current context and commits on a clean
exit or rolls back on an exception. An outbox append and a processed marker recorded inside it join it. A
service that already runs a transaction on its own connection binds it with `store.transaction(conn=conn)`:
the store writes on that connection and never commits or rolls back the service's transaction.

```python
async with pool.acquire() as conn:
    async with conn.transaction():
        async with store.transaction(conn=conn):
            await store.outbox.append(entry)
            await conn.execute("INSERT INTO orders (id) VALUES ($1)", order.id)
```

The store's own transactions (claims, the purge) run at READ COMMITTED. An outbox claim takes
`SELECT ... FOR UPDATE SKIP LOCKED` in `(created_at, seq)` order, so two relays on one outbox never hold the
same entry.

### The layout

The schema files are the published version 1 files, byte for byte, embedded in the package
(`src/snodra_sql/schema/{postgresql,mysql}/V1__snodra.sql`) and checked against their published SHA-256
values. `init_schema()` applies version 1 unless the database has it or later, under the database lock every
Snodra library uses, so instances starting together apply it once. `check_schema()` only checks it, for a
service that runs its own migrations. The add-on also reads `snodra_schema` and refuses a database below
version 1. Schema versions are additive: code that knows version 1 reads a database at version 1 or later.

PostgreSQL 13 or later; MySQL 8.0.17 or later on InnoDB with a strict `sql_mode` (MariaDB is not supported).
The store needs a connection pool of at least two connections: a write that commits on its own inside the
service's transaction takes a second one.

### Lifecycle

`App.start()` drives the store's lifecycle when it is the app's store: it hands the store the application's
clock, calls `init_schema()` (or `check_schema()` when `Settings.store_init_schema` is false) and starts
`background()` in a `snodra:store-background` task until shutdown. A failing schema step fails `start()`, naming
the store; an error from `background()` is logged and the app keeps running.

- `use_clock(clock)` sets the application's clock (epoch milliseconds), which stamps every time the store
  sets itself: an entry's sending, a claim's lease, a purge bound.
- `init_schema()` prepares the schema; `check_schema()` fails unless it is there. They are public for a store
  constructed after `app.start()`, or used outside an `App`.
- `background()` runs the retention purge every `purge_interval` seconds until it is cancelled. A negative
  interval turns it off and it waits for cancellation.
- `open()` opens the pool on demand; `close()` closes the pool and the PostgreSQL listener.

On PostgreSQL an append's transaction sends `pg_notify('snodra_outbox', '')` and `outbox.on_append(wake)`
listens on a connection of its own, so a relay claims as soon as an append commits; a rollback notifies
nobody. MySQL has no such notification and the relay polls.

Where the public specification leaves a choice, [SPEC-GAPS.md](../../SPEC-GAPS.md) records what this library
does. The driver behaviour the add-on relies on is recorded in [DRIVER-SPIKE.md](DRIVER-SPIKE.md).

## Test

The tests start PostgreSQL and MySQL through Testcontainers (pinned by digest) and run the message-store
conformance kit against both. Docker is required:

```sh
uv run pytest -q packages/snodra-messaging-sql/tests
```

## Links

- Documentation: <https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/>
- Repository: <https://github.com/snodra/snodra-messaging-python>
- PyPI: <https://pypi.org/project/snodra-messaging-sql/>
