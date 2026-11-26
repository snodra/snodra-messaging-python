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
  pending count and the processed markers asked about.

The probe observes the library's processed-event markers by wrapping the message store's processed
port; the consumer, its transaction and the relay are the library's. The harness starts and stops the
service and creates its topics, streams and databases; it runs with `PYTHON_SRC` or `PYTHON_REF` from
`interop/run.sh`. `store_interop.topology` is the one-shot init container that creates the RabbitMQ
super stream through the stream client the RabbitMQ add-on uses (a library never creates streams).

Tests: `uv run pytest -q examples/store-interop/tests` from the repository root (the settings, the
ledger and the observed store; no Docker).
