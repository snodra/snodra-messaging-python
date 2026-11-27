# snodra-messaging-mongo

The MongoDB message store for the Snodra messaging suite: the outbox, the processed-event markers and the rest of the
message-store ports in the service's own MongoDB database, in the collections and with the document fields the
published [MongoDB layout](https://snodra.damoreira.com/docs/library-authors/mongodb-layout/) fixes, so documents
written by any Snodra library stay readable without a migration.

## Install

```sh
pip install snodra-messaging-mongo
```

The distribution needs Python 3.12 or later and depends on `snodra-messaging` and PyMongo's async client. The core
library never imports a MongoDB client; this add-on is where it lives.

## Use

```python
import asyncio

import snodra
from snodra_mongo import MongoConfig, MongoStore


async def main() -> None:
    store = MongoStore(MongoConfig.from_env())
    try:
        settings = snodra.Settings.from_env(service="orders-service")
        # The event store reads what the relay publishes; the message store holds the outbox and markers.
        app = snodra.App(settings, store=store, event_store=...)
        await app.run()
    finally:
        await store.close()


asyncio.run(main())
```

`MongoConfig.from_env()` reads `SNODRA_STORE_MONGO_URI` and `SNODRA_STORE_MONGO_DATABASE`; the URI is required unless
the store is given a client. A service that owns its PyMongo client passes it to `MongoStore(config, client=client)`
with an empty `url`: the store does not close a client it does not own.

The store's transactions are MongoDB multi-document transactions on a client session, so the database must be a
replica set (a single-node one is enough).

The store implements every message-store port: the outbox, the processed-event markers, the dead letters, the
processor control state, the external-call records and the saga records. The level-3 and level-4 store checks pass on
a replica set; level 4 is not claimed publicly until the release that publishes it for every library.

### The transaction is the service's

`async with store.transaction():` begins a transaction on a client session, binds it to the current context and
commits on a clean exit or rolls back on an exception. An outbox append and a processed marker recorded inside it join
it. A service that already runs its own MongoDB transaction binds that session with
`store.transaction(session=session)`, and a handler that writes its own documents in the handled event's transaction
asks for the session with `store.session(tx)`:

```python
async with store.transaction() as tx:
    await store.outbox.append(entry)
    session = store.session(tx)
    await store.database["orders"].insert_one(order, session=session)
```

A claim, a marker's existence check, the marks, the purges and an append outside a transaction are writes of their
own that commit on their own, never in the caller's transaction. A duplicate key, a write conflict and an error
MongoDB labels a transient transaction error are `snodra.Conflict` (a retry of the whole attempt resolves them); every
other driver error is `snodra.MessageStoreFailed`.

### The layout

`init_schema()` creates every index and TTL index of the layout, idempotently: creating them creates the collections a
transaction writes to before the first transaction does. Neither an index that exists nor its options change, so every
instance sharing a database must be given the same retentions. `check_schema()` only checks the collections and
indexes, for a service that prepares its schema itself; a TTL that differs from the store's settings counts as a
schema failure. TTLs are stored in whole seconds, floored as the published retentions are.

`App.start()` drives the store's lifecycle when it is the app's store: it hands the store the application's clock,
calls `init_schema()` (or `check_schema()` when `Settings.store_init_schema` is false) and starts `background()` in a
`snodra:store-background` task until shutdown. The methods are public for a store constructed after `app.start()`, or
used outside an `App`. `background()` runs nothing: MongoDB's TTL monitor removes expired documents on the server.
`use_clock(clock)` sets the application's clock (epoch milliseconds), which stamps every time the store sets itself:
an entry's sending and failure, a claim's lease.

Where the public specification leaves a choice, [SPEC-GAPS.md](../../SPEC-GAPS.md) records what this library does.

## Test

The tests start a single-node MongoDB replica set through Testcontainers (pinned by digest) and run the message-store
conformance kit against it. Docker is required:

```sh
uv run pytest -q packages/snodra-messaging-mongo/tests
```

## Links

- Documentation: <https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/>
- Repository: <https://github.com/snodra/snodra-messaging-python>
- PyPI: <https://pypi.org/project/snodra-messaging-mongo/>
