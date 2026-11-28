<p align="center">
  <img alt="Snodra Messaging for Python mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Python)</h1>

<p align="center">
  Commands, queries, events, streams, scatter-gathers, subscriptions, sagas and idempotent external calls for asyncio
  services, routed by Snodra Server.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-messaging-python/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-messaging-python/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://pypi.org/project/snodra-messaging/"><img alt="PyPI" src="https://img.shields.io/pypi/v/snodra-messaging"></a>
  <img alt="Python" src="https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white">
  <a href="https://snodra.damoreira.com/docs/messaging/python/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

One package, `snodra`, that gives an asyncio service what it needs to send and serve requests in a Snodra system, with the
same wire protocol as services written with any Snodra library: you write async functions, the library keeps the hub streams, permits,
retries and timeouts.

- **Commands and queries** through Snodra Server, with deadlines, retries and routing keys. A command or a query that
  is not shared, of a type the service handles itself, is answered in-process by its own handler, without the hub;
  `Settings(always_route_through_hub=True)` sends it through the hub like any other request.
- **Every query mode**: point, scatter-gather, streaming and subscription; streams and updates are async iterables (`async for`).
- **Events through the outbox**: publish inside the same transaction as the service's own writes; a relay publishes what the
  outbox holds, at least once, and event handlers commit their processed-event marker with their work.
- **Event processors and their status**: handlers run in the `default` processor or in a named one
  (`processor="orders"`), each with one subscription and `processor:<processor>:<eventId>` markers, so a redelivery is
  skipped and one processor's failure does not hold another's. Every started processor reports its state, segments,
  counters and dead-letter count to the hub.
- **Processor control**: the dashboard pauses and resumes a processor persistently; a leased reset stops its
  subscription, runs the `@app.reset_handler` handlers before anything is deleted or replayed, deletes the replayed
  range's markers, and replays from earliest, latest, timestamp or explicit positions. A handler declared with
  `replay=False` stays out of a replay, and control runs off the hub read loop.
- **Dead letters**: `Settings.dead_letter_mode` (`SNODRA_EVENTS_DEAD_LETTER_MODE`) parks a record that exhausts its
  attempts in `collection` (the message store's dead letters, with list, retry, retry-all and discard from the
  dashboard), publishes it to the `<source>.dlt` source with the `snodra-dlt-*` headers in `event-store` (when the
  store declares `native-dead-letter`), or logs it and moves on in `log`.
  `Settings.dead_letter_retention` (`SNODRA_EVENTS_DEAD_LETTER_RETENTION_MS`, 30 days by default) is how long a
  collected letter is kept.
- **Sagas**: `@app.saga("payment")` classes with `@saga_start`, `@saga_event` and `@saga_command_failed` handlers,
  associations, a start key that keeps one live instance per order, an optimistic state version and parked failures.
  A handler records through `SagaContext` (`send`, `publish`, `associate_with`, `end`) inside the saga's
  transaction; a relay dispatches each saga's commands and events after it commits, in order under the saga's lease.
  A command the relay gives up on comes back as the local `SagaCommandFailed` notice.
- **Saga deadlines**: `ctx.schedule(...)` records a deadline and returns its id; `ctx.cancel_deadline(id)` and
  `ctx.cancel_deadlines(name)` cancel one or every deadline of a name, and `@saga_deadline("charge-timeout", end=True)`
  handles it. A leased scheduler claims each due deadline, runs its handler and marks it fired in one transaction.
- **Idempotent external calls**: `@app.idempotent_command_handler` records each call in `external_calls` under the
  `idempotency-key` header before the body runs: the first call wins, a repeat after success returns the first reply,
  a permanent failure is replayed, and a lapsed call is given up as unknown (or taken over with
  `external_idempotent=True`). `Retryable` and `UnknownOutcome` write the two reserved `Error.details` markers, which
  `is_retryable` and `is_unknown_outcome` read locally and from a hub; the saga relay owns the backoff.
- **Event and message stores**: Kafka or RabbitMQ Streams for events; MongoDB, PostgreSQL or MySQL for the outbox, the
  processed-event markers, the dead letters and the processor control state. Each store is a separate package, selected
  when the service is built.
- **The hub connection**: registration, heartbeats, reconnects, flow control, several hubs and an ordered shutdown.
- **Credentials and identity**: a token (static, or a file read again), an OAuth2 JWT (renewed) or a client
  certificate (rolled over to a renewed pair); the caller's token verified against the issuer's keys, and guards on
  handlers.
- **Telemetry** behind an interface, with an OpenTelemetry implementation in the separate `snodra-messaging-observability` package.
- **Feature level 4**, proved by the 33 level-1 checks of the conformance runner, by the level-2 to level-4
  event-store and message-store kits, and by the cross-language interop run: one saga continued across languages on
  shared stores, with the dispatch lease, deadlines and failure notices.

## Install

```sh
pip install snodra-messaging
pip install snodra-messaging-observability   # optional: OpenTelemetry
pip install snodra-messaging-kafka           # or snodra-messaging-rabbitmq
pip install snodra-messaging-mongo           # or snodra-messaging-sql
```

The packages are on PyPI and need Python 3.12 or later; the core depends only on `grpcio`, `protobuf` and `cryptography`.
An add-on pins the core version it was released with.

## A handler

```python
@app.command_handler
async def place_order(cmd: PlaceOrder) -> OrderReceipt:
    # A request sent while a request is handled carries its correlation id, the caller's token and the trace on.
    stock = await app.query(CheckStock(items=cmd.items), Stock)
    if not stock.available:
        raise snodra.HandlerError("out of stock", details={"orderId": cmd.order_id})
    return OrderReceipt(order_id=cmd.order_id, total=42)
```

A message is a dataclass with `@snodra.message_type("...")`; its payload is JSON by default (see `snodra.Codec`).
Settings are dataclass options, and `snodra.Settings.from_env()` reads the `SNODRA_*` variables.

## An event

An event is a message type too. Build the service with one event store and one message store, publish inside the
transaction that holds the service's own writes, and handle it in the processor that owns the subscription:

```python
@snodra.message_type("orders.OrderPlaced")
@dataclass
class OrderPlaced:
    order_id: str
    total: int


@app.event_handler(processor="orders")
async def on_order_placed(event: OrderPlaced) -> None:
    await project_order(event)


async with store.transaction():
    await app.publish(OrderPlaced(order_id=cmd.order_id, total=cmd.total), key=cmd.order_id)
```

The relay publishes what the outbox holds, at least once; a redelivered event finds its
`processor:orders:<eventId>` marker and is skipped. A handler without `processor=` runs in `default`.

## Documentation

Quick start, the settings map, the API and testing: **<https://snodra.damoreira.com/docs/messaging/python/>**. A
complete service is [`examples/quickstart`](examples/quickstart/). The library speaks the Snodra protocol (`snodra.hub.v1`).
Where the specification is silent, [SPEC-GAPS.md](SPEC-GAPS.md) records what this library does.
Counterparts: <https://snodra.damoreira.com/>.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## Releases

Versions are cut by CI from the commit subjects since the last tag, in Conventional Commits: a `feat` cuts the next
minor, a `fix` or a `perf` the next patch, and `docs`, `test`, `chore`, `ci`, `refactor`, `build` and `style` cut
nothing. Below 1.0.0 a `!` or a `BREAKING CHANGE:` footer counts as a minor; 1.0.0 is declared by hand. Nobody pushes a
tag: only the workflow makes tags.

All six published distributions carry one version and one tag, `vX.Y.Z`. The release commit, `release: X.Y.Z`, writes
that version into the six `pyproject.toml` files of `packages/`, makes each add-on require `snodra-messaging==X.Y.Z`,
and records them in `uv.lock` (`scripts/bump-version.sh`).

That commit is pushed only after `scripts/release-check.sh X.Y.Z` has built the six sdists and the six wheels, run
`twine check --strict` on them, installed all six wheels on Python 3.12 and 3.14, imported every distribution and run
the tests there. It is the check to run by hand when a release commit is in doubt, and it refuses a version PyPI
already has.

Pull before you push: `git pull --rebase` first, because a release leaves a commit by the workflow's bot on `main`.

A failed release is finished, never repeated. **Re-run failed jobs** on the run: the version, the commit and the tag
are already recorded, and the later jobs continue from them; the GitHub release on its own is recreated with the
`release` workflow and the tag. PyPI keeps a version forever, so a wrong release is fixed by the next one, never by
publishing the same version again.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
