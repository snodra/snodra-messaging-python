# Client spike: `rstream` and the RabbitMQ Streams wire rules

Date: 2026-11-26. `rstream` 1.1.1 on Python 3.13, against RabbitMQ 4.4.0 with
`rabbitmq_stream`, `rabbitmq_stream_management` and `rabbitmq_management` (the pinned test image).

## What the wire rules need

A client for this add-on must be able to

1. set the subscribe properties `single-active-consumer=true`, `name=<service>.<processor>`,
   `super-stream=<source>` and `snodra-member=<a random id per instance>`;
2. answer the broker's consumer update (activation and deactivation) with the stored offset, the
   next one to read, and with `first` when none is stored;
3. query and store offsets under the consumer name on each partition's **leader**;
4. publish one AMQP 1.0 message per event, with no producer name, waiting for each publish confirm;
5. reach the management API, which is HTTP.

## What `rstream` offers

- The high-level `SuperStreamProducer` routes a key with `bytes(key, "UTF-16")` and
  `mmh3.hash_bytes` (a **128-bit** murmur3, read little-endian), not the wire rule's x86_32 of the
  key's UTF-8 bytes (`rstream/superstream.py:88-89`). A key would land on another partition than
  the Kotlin, Rust and Go publishers put it on, so the high-level producer is not usable.
- The high-level `Consumer` accepts arbitrary subscribe properties and a
  `consumer_update_listener`, so `snodra-member`, `single-active-consumer`, `name` and
  `super-stream` do reach the broker. Its listener must return the **public**
  `rstream.OffsetSpecification` (exported at the top level); that reply encodes and the
  subscription starts at the stored offset. Returning the different
  `rstream.ConsumerOffsetSpecification` constant does not encode, but that is a type choice, not a
  client limitation.
- The high-level `Consumer` connects to a **random replica** when the stream has any
  (`rstream/consumer.py:201`), while the wire rules say offset reads, read-backs and a reset's
  stores go to the partition's leader, and both official add-ons subscribe on the leader's
  connection. This is the second blocker.
- The low-level `Client` (`rstream/client.py`) exposes `subscribe` with arbitrary properties,
  `consumer_update`, `query_offset`, `store_offset`, `publish`, `declare_publisher` and
  `query_leader_and_replicas`; its `schema` module encodes and decodes the stream frames, and
  `rstream.amqp.amqp_decoder` reads AMQP 1.0 messages. It is the low-level class the wire rules
  name.
- The low-level `Client` (`rstream/client.py`) exposes `subscribe` with arbitrary properties,
  `consumer_update`, `query_offset`, `store_offset`, `publish`, `declare_publisher` and
  `query_leader_and_replicas`; its `schema` module encodes and decodes the stream frames, and
  `rstream.amqp.amqp_decoder` reads AMQP 1.0 messages. It is the low-level class the wire rules
  name.

## Spike result

A throwaway script and the add-on's real-broker tests confirmed, on RabbitMQ 4.4.0:

- the low-level `Client.subscribe` sends all four subscribe properties; the management API's
  `/api/stream/consumers/%2F` lists the consumer with `properties.name`,
  `properties.snodra-member`, `properties.single-active-consumer` and `properties.super-stream`;
- a `ConsumerUpdateResponse` answered with `schema.OffsetSpecification(OFFSET, n)` starts the
  subscription at `n`; `FIRST` starts at the first offset; a chunk that begins before the stored
  offset is skipped by the add-on;
- `query_offset` and `store_offset` work on the partition's leader connection, and a stored offset
  reads back as the next one;
- a Publish frame with one `schema.Message` (our own AMQP 1.0 bytes) and a publisher declared
  without a name is confirmed by `PublishConfirm`; the decoded message carries
  `properties.message-id` as an AMQP string, `properties.content-type` as the symbol
  `application/json`, the symbol-keyed `x-routing-key` annotation, every header as a string
  application property, exactly one data section, and no footer or delivery annotations;
- the management API is plain HTTP and answers with Basic auth.

The high-level path was rejected for its producer's routing and its consumer's replica choice; the
consumer-update reply is not a reason. The add-on therefore pins `rstream` and uses its low-level
`Client` with its frame and AMQP codecs, the same choice the wire rules describe for the official
add-ons ("use each client's low-level client class"). The decision and the client's constraints
are recorded as SG-36 in `SPEC-GAPS.md`.

## Reading a stream's bounds

`rstream`'s protocol schema has no stream-statistics command, so the add-on reads a stream's first
offset and head the way the reference add-ons do: a temporary subscription at `first` and one at
`last`; the first chunk's first offset is the first position, and the last chunk's first offset plus
its record count is the head. A stream is empty only when both reads are silent within 1.5 s; one
silent read beside one that answered is a failure (SG-38).

## What was not used

The high-level classes are not imported by the add-on. `rstream`'s `py.typed` marker is present but
its type hints are not strict; mypy ignores that module in this workspace.
