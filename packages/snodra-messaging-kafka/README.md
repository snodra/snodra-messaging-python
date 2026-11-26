# snodra-messaging-kafka

The Apache Kafka event store for the Snodra messaging suite: publishers append a service's events to
the shared topic and every processor reads them in a consumer group of its own.

## Install

```sh
pip install snodra-messaging-kafka
```

The distribution needs Python 3.12 or later and depends on `snodra-messaging` and `aiokafka`.
The core library never imports a Kafka client; this add-on is where it lives.

## Use

```python
import asyncio

import snodra
from snodra_kafka import KafkaConfig, KafkaEventStore


async def main() -> None:
    settings = snodra.Settings.from_env(service="orders-service")
    events = KafkaEventStore(KafkaConfig.from_env())
    try:
        # The message store keeps the outbox, the processed-event markers and the dead letters;
        # the event store is the Kafka log the relay publishes to and the processors read.
        app = snodra.App(settings, store=snodra.MemoryStore(), event_store=events)
        await app.run()
    finally:
        await events.close()


asyncio.run(main())
```

`KafkaConfig()` defaults to the bootstrap servers `kafka:19092`; `KafkaConfig.from_env()` reads
`SNODRA_KAFKA_BOOTSTRAP` and falls back to that default. `client_id`, `request_timeout_ms`,
`session_timeout_ms`, `max_poll_records`, `status_refresh` and `admin_timeout` are the add-on's own
bounds.

The events source is a topic (`snodra.events` by default), a segment is the partition number in
decimal and a position is the offset in decimal. The record key is the event key, the value the JSON
payload, every event header a record header with the same name, looked up by name and never by place.
A keyed record is partitioned the way the Java client's default partitioner does it (murmur2 of the
key's UTF-8 bytes, masked positive, modulo the partitions), so one key from any Snodra publisher
lands on one partition and keeps its order there.

A subscription joins the consumer group named after `<service>.<processor>` on the classic group
protocol with the `range` assignor, so it shares a group with every other official add-on. Committed
offsets are written explicitly and are the next offset to read; a new group starts at the earliest
offset. The add-on declares `replay`, `lag` and `native-dead-letter`; it has no event browser and
declares no `browse`. Dead letters of the `event-store` mode go to `<source>.dlt`.

The add-on creates no topic: the infrastructure does, and the broker must keep
`auto.create.topics.enable=false` (the add-on's tests pin it off). With auto-creation on, the broker
would create a missing topic on the first publish or subscription. A name no Kafka topic can have, a
record too large for the broker or a refused authorization is rejected for good; a missing topic and
any other failure (a missing topic may appear later) is retried by the outbox relay.

Where the public specification leaves a choice, [SPEC-GAPS.md](../../SPEC-GAPS.md) records what this
library does.

## Test

The tests start an Apache Kafka broker through Testcontainers (pinned by digest) and run the
event-store conformance kit against it. Docker is required:

```sh
uv run pytest -q packages/snodra-messaging-kafka/tests
```

## Links

- Documentation: <https://snodra.damoreira.com/docs/messaging/python/>
- Repository: <https://github.com/snodra/snodra-messaging-python>
- PyPI: <https://pypi.org/project/snodra-messaging-kafka/>
