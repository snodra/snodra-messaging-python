# snodra-messaging-rabbitmq

The RabbitMQ Streams event store for the Snodra messaging suite: publishers append a service's
events to a super stream and every processor reads them as a single active consumer of its own
name, on the layout the published
[RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
fix.

## Install

```sh
pip install snodra-messaging-rabbitmq
```

The distribution needs Python 3.12 or later and depends on `snodra-messaging` and `rstream`. The core
library never imports a stream client; this add-on is where it lives.

## Use

```python
import asyncio

import snodra
from snodra_rabbitmq import RabbitMQConfig, RabbitMQEventStore


async def main() -> None:
    settings = snodra.Settings.from_env(service="orders-service")
    events = RabbitMQEventStore(RabbitMQConfig.from_env())
    try:
        # The message store keeps the outbox, the processed-event markers and the dead letters;
        # the event store is the super stream the relay publishes to and the processors read.
        app = snodra.App(settings, store=snodra.MemoryStore(), event_store=events)
        await app.run()
    finally:
        await events.close()


asyncio.run(main())
```

`RabbitMQConfig()` defaults to the host `rabbitmq`, port 5552 (5551 with TLS), vhost `/`, user and
password `guest`. `RabbitMQConfig.from_env()` reads `SNODRA_RABBITMQ_HOSTS`, `SNODRA_RABBITMQ_PORT`,
`SNODRA_RABBITMQ_VIRTUAL_HOST`, `SNODRA_RABBITMQ_USERNAME`, `SNODRA_RABBITMQ_PASSWORD`,
`SNODRA_RABBITMQ_TLS` and `SNODRA_RABBITMQ_MANAGEMENT_URL`; the timeout and offset settings keep
their defaults.

The library creates no stream. The infrastructure creates a super stream for the events source and,
for the dead-letter mode `event-store`, a plain stream for `<source>.dlt`:

```sh
rabbitmq-streams add_super_stream snodra.events --partitions 6
rabbitmqadmin declare queue --name snodra.events.dlt --type stream --durable true
```

A missing stream fails publishes (the relay retries) and subscriptions (they retry after a backoff).

The events source is a super stream; a segment is the partition stream's suffix (`3` for
`snodra.events-3`), `0` for a plain stream, and a position is the offset in decimal. An event is one
AMQP 1.0 message: the JSON payload as its one data section, the event id as the message id, the key
as the `x-routing-key` annotation, every header an application property. A keyed event goes to the
partition of murmur3 x86_32 of its UTF-8 key (seed 104729, unsigned, modulo the partitions), so one
key from any Snodra publisher lands on one partition and keeps its order there. The stored offset is
the next one to read.

A subscription is the consumer name `<service>.<processor>` on every partition with
`single-active-consumer=true` and a random `snodra-member` per instance, so the broker spreads the
partitions and the management API can tell the instances apart. The add-on declares `lag` and
`native-dead-letter` always; with `management_url` it also declares `replay`, and only then does
`admin()` exist. It has no event browser and declares no `browse`.

Where the public specification leaves a choice, [SPEC-GAPS.md](../../SPEC-GAPS.md) records what this
library does; [CLIENT-SPIKE.md](CLIENT-SPIKE.md) records why the add-on drives `rstream`'s
low-level client.

## Test

The tests start RabbitMQ 4.4 with the stream and management plugins through Testcontainers (pinned
by digest) and run the event-store conformance kit against it. Docker is required:

```sh
uv run pytest -q packages/snodra-messaging-rabbitmq/tests
```

## Links

- Documentation: <https://snodra.damoreira.com/docs/messaging/python/>
- Repository: <https://github.com/snodra/snodra-messaging-python>
- PyPI: <https://pypi.org/project/snodra-messaging-rabbitmq/>
