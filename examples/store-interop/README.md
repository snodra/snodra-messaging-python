# store-interop

The probe service of the cross-language stores interoperability profile
([snodra-conformance](https://github.com/snodra/snodra-conformance)'s `interop/run.sh --profile stores`).

It runs one App per store family on the library's public API: `kafka` and `rabbitmq` on the in-memory
message store, and `postgres`, `mysql` and `mongo` on the matching database add-on (each with Kafka as
its event store). Every App registers the service `store-interop` with the instance id
`<language>-<family>` and one subscription per family, `<service>.interop-<family>`; a process of
another language that registers the same names shares its Kafka consumer group or its RabbitMQ
Streams single-active-consumer name.

The service answers the harness's commands (all neutral orders-service shapes):

- `orders.interop.Who` — the instance, language and family that answered;
- `orders.interop.Publish` — append events to the outbox, which the App's relay publishes;
- `orders.interop.PublishRaw` — publish events through the event store directly;
- `orders.interop.State` — the deliveries its consumer recorded (handled or duplicate), the outbox's
  pending count, its processor's control state, the dead letters its store lists, the dead-letter
  events its probe subscription received and the processed markers asked about;
- `orders.interop.ControlSet` — pause or resume the processor by writing the message store's control
  state directly (the other language's instance reads it on its own poll);
- `orders.interop.Poison` — publish an event its handler rejects until the id is allowed;
- `orders.interop.Allow` — let this instance's handlers take a poison id, so the instance the hub
  picks for a retry can handle it;
- `orders.interop.SagaPublish` — publish a saga started/advanced event on a chosen partition;
- `orders.interop.SagaState` — read a shared saga by id or association (id, state, version, ended);
- `orders.interop.SagaDispatch` — the first pending saga dispatch entry and the pending count;
- `orders.interop.SagaLease` / `SagaRelease` — try or release another relay's dispatch lease;
- `orders.interop.SagaNotice` — this instance's ordinary notice count and whether its notice watch
  saw the notice id in the event store.

Every family registers a processor `<service>.interop-<family>` and a reset handler, and a second
`OrderPlaced` handler with `replay=False` proves a replay leaves it out. The `rabbitmq` family uses
the event-store dead-letter mode: its processor publishes a record no handler can take to
`<source>.dlt`, and the probe subscribes to that source so the driver can assert the six
`snodra-dlt-*` headers, the key and the payload. The other families park collection dead letters,
which the probe's state lists and the driver retries through the hub.

Every family also registers the shared saga `interop-saga`: a start handler that associates `orderId`
and schedules `saga-timeout`, an advanced handler that records a slow command or a command that
fails with `INVALID`, a deadline handler and the local `SagaCommandFailed` handler. The same state
fields and wire names are used in both languages, so a saga Python starts is continued by Go and the
reverse. The slow command's handler tells the driver (`store.runner.SlowStarted`) which language ran
it and keeps the send in flight until the driver answers, so the relay holds the dispatch lease for
the driver's probe. `SagaState` also returns the saga's version and live-instance count; a release is
verified with a witness claim. The notice handler is ordinary (local dispatch); a raw subscription
to the family source counts every published `snodra.SagaCommandFailed` for the saga (there must be
none).

The probe observes the library's processed-event markers by wrapping the message store's processed
port; the consumer, its transaction and the relay are the library's. The harness starts and stops the
service and creates its topics, streams and databases; it runs with `PYTHON_SRC` or `PYTHON_REF` from
`interop/run.sh`. `store_interop.topology` is the one-shot init container that creates the RabbitMQ
super stream and its dead-letter super stream through the stream client the RabbitMQ add-on uses (a
library never creates streams). The released tags carry the level-4 probe, the saga commands included.

Tests: `uv run pytest -q examples/store-interop/tests` from the repository root (the settings, the
ledger and the observed store; no Docker).
