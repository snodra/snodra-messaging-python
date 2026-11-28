# Specification gaps

Places where the [library specification](https://snodra.damoreira.com/docs/library-authors/) is silent, and what this
library does there. Where an entry is settled, its behaviour is this library's rule. The code carries a
`# TODO(spec-gap): SG-<n>` comment at the spot.

## Format of an entry

### SG-<n>: short title

- **Specification:** what the specification says, or that it is silent.
- **This library:** what the Python code does, and in which file.
- **Question:** the open question, or "settled".

## Entries

### SG-1: the error texts a caller reads and a handler writes

- **Specification:** `messages.md` (Handler errors) says an `Error.message` is a short text, "never a stack trace". It
  gives no text for the codes the hub itself produces, and no rule for which exception a handler raises to answer a
  given code.
- **This library:** `snodra/_errors.py` (`error_from_wire`, `error_to_wire`). The caller reads fixed texts for the
  codes the hub produces: `No handler registered for '<type>'` (`NO_HANDLER`), `Timed out waiting for reply to
  '<type>'` (`TIMEOUT`), `Handler for '<type>' disconnected` (`HANDLER_DISCONNECTED`), `Handler for '<type>' is busy:
  its hub queue or session buffer is full` (`RESOURCE_EXHAUSTED`), `cancelled by the hub` for an empty `CANCELLED`
  message, and the wire message for every other code. A scatter-gather part's `error` keeps the message the target or
  the hub gave for every code (`snodra/_gather.py`, `_part_error`), and an unknown code reads as `HANDLER_ERROR` with
  its details kept. A handler's `Unauthenticated`, `PermissionDenied` and
  `InvalidRequest` answer their code with their own message; `HandlerError(message, details=...)` answers
  `HANDLER_ERROR` with JSON details; a `ValueError` answers `HANDLER_ERROR` with its plain message and the details
  `[]`; any other exception answers `HANDLER_ERROR` `<ClassName>: <message>`. Every line break in a message becomes
  one space (the conformance contract's rule for `L1-HANDLER-ERRORS`); details are set only with `HANDLER_ERROR`; a
  failure of a call the handler made is its own `HANDLER_ERROR`.
  A guarded handler (`snodra/_dispatch.py`, `Dispatcher.answer_checked`) refuses an anonymous caller with
  `UNAUTHENTICATED` `handler <name> requires authentication`, a caller its rule refuses with `PERMISSION_DENIED`
  `handler <name>: access denied`, and a caller whose rule raised with `PERMISSION_DENIED`
  `handler <name>: access denied (<ClassName>)`, `<name>` being the handler function's qualified name. A forwarded
  token the JWKS verifier refuses is answered `UNAUTHENTICATED` with one of the verifier's short reasons
  (`snodra/_jwt.py`): `unreadable token`, `unsupported token algorithm`, `unsupported critical header`, `token has
  no kid`, `signing keys unavailable`, `unknown signing key`, `unusable signing key`, `invalid token`, `token has no expiry`,
  `token expired`, `token not valid yet`, `token issuer not accepted`.
- **Question:** settled.

### SG-2: the forms of a hub address

- **Specification:** `protocol.md` and `writing-a-library.md` name the hubs a library connects to, `host:port` targets
  and one DNS name whose addresses are the hubs ([Several hubs](https://snodra.damoreira.com/docs/library-authors/protocol/)),
  but no syntax for the configured addresses.
- **This library:** `snodra/_settings.py` (`hub_targets`, run when `Settings` is built) takes `host:port` or
  `static://host:port`, several of them (repeats dropped, order kept; `SNODRA_SERVER_ADDRESS` is a comma list whose
  blank entries are dropped), or exactly one `dns:///name:port` with a port from 1 to 65535. Every other `scheme://` entry, `http://` and `https://` included, is
  refused with a `ConfigurationError` that names `SNODRA_SERVER_ADDRESS` and the accepted forms. TLS is a setting of
  its own, for every hub.
- **Question:** settled: an address with another scheme is refused at startup.

### SG-3: the texts of a request that was never sent

- **Specification:** `protocol.md` ("Several hubs") quotes the beginnings of the texts, "no hub connected within ..."
  and "no hub took the request within ...", and the whole `'<type>' not written: 9 hub streams lost before it was`. It
  does not say how the duration is written.
- **This library:** `snodra/_send.py` (`Sender._route`) writes `no hub connected within <ms> ms` when no hub was
  tried, `no hub took the request within <ms> ms` when one was, and
  `'<type>' not written: 9 hub streams lost before it was`, the duration in milliseconds (for example
  `no hub connected within 300 ms`). A call made once the shutdown drained the handlers fails at once with
  `'<type>' not sent: the app is shutting down`.
- **Question:** settled: the duration is written in milliseconds.

### SG-4: the client certificate rollover

- **Specification:** `protocol.md` (Credentials) says a library must reload the certificate files and reconnect with
  the renewed pair before the presented certificate's `notAfter`, and `writing-a-library.md` (Credentials) that the
  official libraries "watch the files, wait a random delay so replicas do not all leave at once, and roll their hubs
  over one at a time". It names no period, no bound of the delay and no limit of a hub's roll.
- **This library:** `snodra/_tls.py` (`Certificates.watch`, `roll_delay_bound`) and `snodra/_connection.py`
  (`HubConnection.roll_over`). The files are read at every connect and every 3 s; a pair that differs from the one
  presented and is valid (the same checks as at start, past its `notBefore`, before its `notAfter`) is adopted. Once
  per pair, before its first roll, the instance waits a delay uniform in `[0, min(30 s, a tenth of what the presented
  certificate has left)]` and reads the files again. Then the hubs whose registered stream presents an older pair are
  rolled in the order of the configured addresses, one at a time: a `Register` with no types and its `Heartbeat`, at
  most 10 s for the stream to be idle (the hub echoed that heartbeat, no handler of a request it forwarded is running
  and no request sent on it waits for its answer), the half-close (at most 2 s for the hub's end), and a new stream
  at once with the new pair. The next hub is rolled only once that stream is acknowledged, within 30 s; otherwise the
  rollover to that pair stops (one error) until the files change again. A hub being rolled takes no new request while
  another hub is live. A pair that cannot be used is kept out (logged at debug when first seen, at error when seen
  again) and the pair presented stays.
- **Question:** open: should the specification name the period, the delay's bound and the limits, or leave them to
  each library?

### SG-5: the next hub of an unkeyed request that one did not take

- **Specification:** `protocol.md` ("Several hubs") fixes the rendezvous order for a keyed request, and says the order
  among the untried hubs for an unkeyed request is not fixed: the official libraries differ with three or more hubs,
  and a caller must not rely on which hub comes next.
- **Other Snodra libraries:** they differ, as the specification says: one takes the untried live hubs as a list and
  picks `list[counter % size]` with its shared round-robin counter, another takes a new round-robin turn over all live
  hubs and picks the first hub not tried.
- **This library:** `snodra/_pool.py` (`Pool.pick`) and `snodra/_send.py` (`Sender.route`). An unkeyed request takes
  the live untried hubs as a list and picks `candidates[counter % len(candidates)]`, one counter shared by every call
  and advanced by every pick (the first hop included); a keyed one takes the untried live hub with the highest
  rendezvous score.
- **Question:** settled: the reference implementation's order, pinned by
  `test_the_unkeyed_next_hub_is_the_reference_order` and by the `L1-HUB-FAILOVER` check.

### SG-6: telemetry instrumentation scope name and request span

- **Specification:** `writing-a-library.md` (Observability) and `protocol.md` describe the metrics and trace
  propagation across hub requests, but do not specify an instrumentation scope name or whether handled requests start a
  server span.
- **Other Snodra libraries:** they differ: some create spans only through an external tracing bridge, while others
  provide direct OpenTelemetry instrumentation with a dedicated scope name and server spans.
- **This library:** `packages/snodra-messaging-observability` (`src/snodra_observability/__init__.py`). The instrumentation
  scope name is `"snodra"`. Each handled request (command or query) runs inside a `SERVER` span named after the
  request's message type, as a child of the request's incoming `traceparent` (and `tracestate`). An outbound request
  sent during the handling of a request injects the active span context through the configured propagator into its
  headers.
- **Question:** open: should the specification standardize the instrumentation scope name and span attributes across
  all Snodra libraries?

### SG-7: JSON details of the default payload codec

- **Specification:** `messages.md` (Payloads) fixes camelCase names, UTC ISO 8601 timestamps, string ids, unknown fields
  ignored and `{}` for a command without a result. It is silent on numbers, `null` against an absent field, enum and
  date-time forms, big integers and bytes.
- **This library:** `snodra/_codec.py`. It writes `null` fields, fails on the whole-payload `null`, and reads an empty
  payload as no value only into `None`. A `datetime` is written in UTC with 0, 3 or 6 fraction digits (a
  naive `datetime` is refused) and read with or without up to 9 fraction digits (the digits beyond microseconds are
  dropped) and a `Z` or an offset, never without a zone. `bytes` is standard base64. It takes no coercions: an
  integer field takes only a JSON integer (never `1.0`, `1.5`, `"1"` or `true`), a float field an integer or a float
  (never a string), a string field only a string, a boolean field only a boolean. An absent field of an optional type
  (`X | None`) reads as `None`; any other absent field without a default fails. A number too large for a float
  (`1e999`, a 400-digit integer) is refused on read, `NaN` and `Infinity` on read and write. An enum is written by its
  member value: another service reads it as an enum only when the value is the name of one of its constants. A `set`
  is written sorted (or, when its elements do not sort, by their JSON text) so equal sets give equal bytes. One
  trailing underscore of a field name is dropped (`type_` is `type`). Python integers have no range, so a value
  beyond signed 64 bits is written as it is.
- **Question:** open: should a reader accept coercions (`1.0` or `"1"` into an integer field, a number into a string
  field, `"true"` into a boolean field)?

### SG-8: a bound on a stream's outbound requests

- **Specification:** silent. `protocol.md` ("Never sent or outcome unknown") speaks of the library's own outbound
  buffer, but sets no size for it.
- **This library:** `snodra/_send.py` (`Sender._hop`) holds at most 256 requests in a stream's outbound buffer
  (`Timings.outbound_limit`); a call beyond waits for room within its deadline, and is woken when the stream ends or
  the app stops sending. Heartbeats, replies and permits are never held back.
- **Question:** open: should the specification bound the buffer, or leave unbounded the memory a stalled stream may
  hold?

### SG-9: an empty correlation id

- **Specification:** `messages.md` (Correlation id) says that when the message being handled "carries none", its own
  id stands in, and that a deadline with an empty correlation id is read as one without. It does not say whether a
  request whose `correlationId` header is present but empty carries one.
- **This library:** `snodra/_dispatch.py` (`Dispatcher._call`) and `snodra/_propagation.py`. A request without the
  header has its id as its correlation id; a request with an empty header keeps the empty string: the handler's
  `current_message().correlation_id` is `""`, and the requests it sends carry `correlationId: ""` on. **An event
  published while handling a message whose correlation id is empty gets a fresh UUID instead** (`snodra/_events.py`,
  `event_headers`): an event never carries an empty `correlationId`, as Go and Rust also write a new one when there is
  none. The request side keeps the empty value because SG-9's question is open; the event side follows the
  specification's "a new UUID when there is none".
- **Question:** open: should an empty `correlationId` header count as none, so that the request's id stands in?

### SG-10: a scatter-gather a hub answers with `NO_HANDLER`

- **Specification:** `protocol.md` ("Several hubs") says that a unary request a hub answers with `NO_HANDLER` goes to
  the other live hubs in turn, and that every call kind follows the failover rules; it does not say whether a
  scatter-gather that a hub answers with `NO_HANDLER` (no live target there) goes to another hub.
- **This library:** `snodra/_gather.py` (`gather`). A gather moves to another hub only while it was not taken (not
  written); a hub's `NO_HANDLER` for it is the answer, as for a gather whose type has no live target anywhere. A retry
  policy may still send a new gather, which picks a hub anew.
- **Question:** open: should a gather walk the other live hubs after `NO_HANDLER`, as a unary request does?

### SG-11: the least wait between two token renewals

- **Specification:** silent: the renewal margin is specified (`max(lifetime / 5, 5 s)`, at most half the lifetime),
  the pacing of renewals is not, so a token shorter-lived than its own margin leaves the question open.
- **Other Snodra libraries:** they differ. At least one waits at least 1 s between two renewals (its retry floor,
  documented as the least wait between renewals); another relies on the hub's refresh throttle; the reference
  implementation has no floor, so it renews a sub-second token in a loop.
- **This library:** at most one renewal per second whatever the lifetime: `snodra/_credentials.py`
  (`MIN_RENEWAL_INTERVAL`, applied in `TokenProvider._publish`). A token shorter-lived than that is handed out until
  the next renewal; one a hub refuses with `UNAUTHENTICATED` is fetched again at once, not held back.
- **Question:** open: should the floor stay (it keeps a sub-second token from fetching in a hot loop), or should such
  a token be renewed at its margin, as the reference implementation does?

### SG-12: a subscription that ends before its initial result

- **Specification:** `protocol.md` ("Subscription") says the hub numbers a subscription's parts itself, `1` for the
  initial result, and names the sender's endings (a clean end, an end with an error, a lost part); it does not say what
  a sender does with a clean `ReplyEnd` that arrives before any part.
- **Other Snodra libraries:** they refuse such an answer as a local protocol error, not with a hub code, since no
  handler produced a result to report.
- **This library:** `snodra/_subscribe.py` (`Subscription._attempt`). The wait for the initial result ends with
  `HubError` `INVALID_REQUEST` `subscription <type> ended before its initial result`: the subscription never opened, so
  there is no initial result to return; an end carrying an error is that error. A first part whose `seq` is not `1` is
  a lost part instead (`LostParts`, as for a stream), and the subscription is cancelled.
- **Question:** open: should such an end read as a loss (`LostParts`) or as this local contract failure?

### SG-13: the wait after a failed first JWKS load

- **Specification:** silent. `messages.md` (Forwarded identity) says a forwarded token is verified against the issuer's
  JWKS, and that a token fails while the endpoint is down (`UNAUTHENTICATED`), but it sets no pacing for fetching the
  keys: neither their cache nor a retry after a failed load.
- **Other Snodra libraries:** they differ: some retry a failed first load on every token until it succeeds, while
  another leaves the retry pacing to its JWT decoder.
- **This library:** `snodra/_jwt.py` (`JwksVerifier._load`, `_retry_wait`). After a first load that failed, the next
  one starts after `first_retry` (1 s, doubling with each failure in a row, never above `jwks_min_refresh`); tokens in
  between are refused `UNAUTHENTICATED` `signing keys unavailable`. Once a load has succeeded, an unknown `kid`
  fetches the keys at most once per `jwks_min_refresh`.
- **Question:** open: should the specification name the backoff after a failed first load, or leave it to each
  library? (a) name the 1 s doubling wait, capped at the refresh interval; (b) retry on every token, as some libraries
  do; (c) leave it open. Recommend (a): a down JWKS endpoint should not get one fetch per forwarded token, and the
  wait is capped at the normal refresh interval anyway.
- **Wire change:** no.

### SG-14: a redelivery that does not move a held segment's position back

- **Specification:** `event-store-add-ons.md` says `redeliver(segment, position)` "hands that delivery and the
  segment's later ones out again without leaving the subscription". It does not say what a store does with a position
  that is not one the subscription has delivered from a segment it holds: a position ahead of its next one, or a
  delivery of a segment that was revoked since.
- **Other Snodra libraries:** they differ. One validates the position against the segment's head: a position at or
  past the head is `Invalid`, while a position still inside the segment moves the cursor to it even when it is ahead
  of the next one, so the events between are skipped; a segment the subscription no longer holds is ignored. Another
  checks no head and moves the cursor only back: a position at or after the next one, or a segment no longer held,
  changes nothing.
- **This library:** `snodra/_eventstore_memory.py` (`_MemorySubscription.redeliver`). The segment, the canonical
  position and the head are validated (an unknown segment or a position at or past the head is `Invalid`), and then
  the cursor moves only back: a position ahead of the next one, or a segment the subscription no longer holds, changes
  nothing. An acknowledgement is validated the same way.
- **Question:** open: when the segment is no longer held, should a redelivery be an error (`Invalid`/`NotOwner`) or a
  no-op as here; and should a position ahead of the next one move the cursor forward or be a no-op?

### SG-15: the append times of the in-memory event store when the clock steps back

- **Specification:** `event-store-add-ons.md` says an in-memory event's time is the store's clock, and a reset to a
  time moves each segment to "the first position at or after" it. It does not require a segment's append times to be
  nondecreasing, nor say what a time-targeted reset does when they are not (the first position at or after a time is
  ambiguous if a later event carries an earlier time).
- **Other Snodra libraries:** they differ: one keeps the clock's value as it is, so a backward clock step can make a
  later event older; another keeps the append times of a segment nondecreasing.
- **This library:** `snodra/_eventstore_memory.py` (`MemoryEventStore.publish`). An event is stamped with the later of
  the segment's last time and the clock, so a segment's append times never go back and a time-targeted reset always
  has one meaning. The store takes the clock as an injectable keyword argument for tests.
- **Question:** open: should the specification require nondecreasing append times, or define a time-targeted reset
  without relying on them?
- **Wire change:** no.

### SG-16: a second outbox append of an entry id that exists

- **Specification:** `message-store-add-ons.md` (Outbox) says an entry is pending until it is sent or failed and what a
  claim returns; it does not say what an append does with an id the outbox already holds (a retry of a transaction whose
  commit is unknown, a publisher that reuses an event id).
- **Other Snodra libraries:** another official library's in-memory store throws its duplicate-key failure at once
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryStores.kt:108`);
  the SQL add-ons' primary key on the entry id does the same.
- **This library:** `snodra/_store_memory.py` (`MemoryStore.open_outbox`) raises `Conflict` and appends nothing, on its
  own or in a transaction; a database add-on answers the same through its schema. Pinned by
  `test_a_second_append_of_one_entry_id_conflicts`.
- **Question:** open: should a duplicate append be a conflict (as here) or an idempotent no-op? Recommend the conflict:
  the id names one event, and a silent no-op would let two different events share it.
- **Wire change:** no.

### SG-17: when a second live writer of one record is refused

- **Specification:** `message-store-add-ons.md` (Transactions) says two live transactions writing one record conflict
  and that the library retries the attempt; it does not say whether the second writer is refused at its write or at its
  commit. The reference kit accepts either for a database store.
- **Other Snodra libraries:** another official library's in-memory store refuses the second write at once with its
  concurrency failure (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/store/memory/MemoryTables.kt:79`),
  and its counterpart in the other official library does the same; a database may report it at the write or at the
  commit.
- **This library:** `snodra/_store_memory.py` (`MemoryStore._reserve`) refuses the second live writer's write with
  `Conflict` and marks that transaction unusable, so its commit is a `Conflict` too; a transaction whose record changed
  after it wrote it conflicts at its commit. Pinned by `test_two_live_writers_of_one_record_conflict`.
- **Question:** open: should the specification settle that the second write is refused at once, or leave the moment to
  the store as the kit does?
- **Wire change:** no.

### SG-18: an event or a subscription without a source or a name

- **Specification:** `event-store-add-ons.md` says `publish(event)` appends an event to its source and `subscribe(name,
  source)` joins a subscription on a source; it does not say whether an empty source, name or both are valid, nor what
  a store does with them.
- **Other Snodra libraries:** they differ: one accepts an empty source or name as any other; another refuses a publish
  without a source and a subscription without a name and a source.
- **This library:** `snodra/_eventstore_memory.py` (`MemoryEventStore.publish`, `subscribe`). The in-memory store
  refuses a publish with an empty source with `Rejected("an event needs a source")` and a subscription with an empty
  name or source with `Invalid("a subscription needs a name and a source")`; nothing is appended or joined.
- **Question:** open: should the specification refuse empty sources and names everywhere, or leave the empty string a
  valid name?
- **Wire change:** no.

### SG-19: the longest outbox send, and the lease margin around it

- **Specification:** `writing-a-library.md` (Level 2, Relay) says the relay claims entries under a lease, hands each to
  the event store and marks it sent once accepted; `message-store-add-ons.md` (Outbox) says a lease holds a batch and
  that a failed attempt keeps the entry pending. Neither says how long one publish may take, how much of the lease a
  batch must leave free for a send, nor what happens to a publish that never answers.
- **Other Snodra libraries:** they differ. The Kotlin outbox publisher waits 45 s for one publish and refuses a lease
  no longer than that (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/OutboxPublisher.kt:197`,
  the check at line 65, the batch deadline at line 85); the Rust relay waits 40 s (`OUTBOX_SEND_TIMEOUT`).
- **This library:** `snodra/_outbox.py` (`OUTBOX_SEND_TIMEOUT`, `OutboxRelay.relay_once`). One publish that has not
  answered within 40 s counts as a failed attempt; a batch stops starting a send once its lease has less than a send
  left (the next poll claims again and renews the lease); a `Settings.outbox_lease` of 40 s or less is refused when the
  settings are built, naming `SNODRA_OUTBOX_LEASE_MS`. The relay follows the Rust reference's 40 s.
- **Question:** open: should the specification name the send timeout and the lease margin, or leave them to each
  library? Recommend naming both once, so an add-on's event store can size its own timeout below the library's.
- **Wire change:** no.

### SG-20: the attempts and the wait of a failing event handler at level 2

- **Specification:** `writing-a-library.md` (Level 2, Consume) says a failing handler is "retried with backoff, then the
  record becomes a dead letter (level 3) or, at level 2, is logged and skipped". It names neither the attempts nor the
  wait, nor whether the stage-1 marker of a record whose attempts are exhausted stays out of the store.
- **Other Snodra libraries:** the Kotlin consumer invokes a handler five times in all, one second apart, and gives the
  record up after the fifth (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:97`,
  line 98, lines 455-472; the default five is `SnodraMessagingProperties.kt:314`); the Rust consumer does the same with
  its own constant.
- **This library:** `snodra/_consumer.py` (`EventConsumer._handle`) and `snodra/_settings.py`
  (`consumer_max_attempts`, `SNODRA_CONSUMER_MAX_ATTEMPTS`, five by default; `Timings.event_handler_backoff`, 1 s).
  Each attempt runs the stage-1 marker and the handlers in one transaction, so a failed attempt commits nothing (no
  marker, no published event). After the last attempt the record is logged at error, without its payload, and
  acknowledged as the level-2 rule says; level 3 replaces this with a dead letter.
- **Question:** open: should the specification name the level-2 attempts and wait, or leave them to each library?
  Recommend naming both, since the wait is what a service sees when its handler is down.
- **Wire change:** no.

### SG-21: the wait before subscribing again after a failed event subscription

- **Specification:** `event-store-add-ons.md` says a failing `next` ends a subscription and the caller subscribes
  again after a backoff; it names neither the first wait, nor its cap, nor when it starts over.
- **Other Snodra libraries:** they differ in the reset rule. The Kotlin reader waits 1 s doubling to 30 s while
  subscriptions keep failing before they delivered anything, and starts over from 1 s after a subscription that
  delivered an event or lived 30 s (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/Resubscribe.kt:65`);
  the Rust consumer has its own constants.
- **This library:** `snodra/_consumer.py` (`EventConsumer.run`) with `Timings.event_resubscribe_min` (1 s),
  `event_resubscribe_max` (30 s) and `event_resubscribe_stable` (30 s): the wait is slept first and doubled only after
  the sleep, so the first retry waits the minimum; it starts over at the minimum once a subscription delivered a record
  or lived the stability window before it failed. A close that fails is logged and does not stop the resubscribe.
- **Question:** open: should a long-lived subscription without a delivery reset the wait, as the Kotlin reader does
  with its 30 s stability window? Recommend yes: the Kotlin reference treats a subscription that lived 30 s as one
  that worked, and this library does the same.
- **Wire change:** no.

### SG-22: what a level-2 library does with a record it can never handle

- **Specification:** `writing-a-library.md` (Level 3, Dead letters) says a record without a type or with a payload that
  is not a JSON object is an `INVALID` letter (discard only) and that a record whose handler cannot read its payload is
  not retried. At level 2 the specification says only that a record failing its attempts is "logged and skipped"; it
  does not say what a malformed record does, nor whether it gets a stage-1 marker.
- **Other Snodra libraries:** the Kotlin consumer's `log` sink logs the record (never its payload) and acknowledges it
  without a marker; an `InvalidMessageException` from a handler's decode breaks its attempt loop at once
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:463`,
  the invalid path at line 521).
- **This library:** `snodra/_consumer.py` (`EventConsumer._handle`). A record without a type header (or with a blank
  one) and a record whose payload is not a JSON object are logged at warning, without the payload, and acknowledged
  with no handler run and no marker. A payload of a handled type that the codec cannot read as the handler's message
  (`DecodeError`) is logged at error and acknowledged after that one attempt: the marker recorded before the decode
  rolls back with the transaction, so it leaves no marker and no handler effect, and the other attempts are not made.
  Any other handler failure is retried as SG-20 says.
- **Question:** open: should the level-2 rule name the malformed record explicitly, and should a decode failure be
  tried once (as Kotlin breaks at once) or with the other attempts? Recommend naming both as this library does: a
  decode failure is logged and skipped once, without a marker.
- **Wire change:** no.

### SG-23: the seven event headers when nothing being handled carries a trace context

- **Specification:** `messages.md` (Event headers) says every event carries seven headers, `traceparent` being "the
  W3C trace context of the work that published it", and that a library must set them all on every event it publishes.
  An event published outside any handling (a scheduler, a tool, a startup task) has no trace context to carry, and the
  page does not say whether the header is then absent or empty.
- **Other Snodra libraries:** they write `traceparent` only when a valid one exists: the Kotlin bus injects the trace
  headers from the current context and writes none when there is none
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/OutboxEventBus.kt:58`);
  the Rust relay adds it only when the publishing context carries a valid one.
- **This library:** `snodra/_events.py` (`event_headers`, `outgoing_event`). Six headers are always written, with a
  non-empty `correlationId`; the seventh, `traceparent`, is written when the message being handled carries a valid one
  (and telemetry may supply a child), and omitted when there is none. An empty `traceparent` is never written: it is
  not a W3C trace context.
- **Question:** open: should the specification allow the six headers when no trace context exists, as every official
  library does, or require an empty `traceparent`? Recommend the former; a fabricated or empty trace id is worse than
  its absence.
- **Wire change:** no.

### SG-24: a paused Kafka subscription and the client's bound on the time between polls

- **Specification:** `event-store-add-ons.md` says a subscription that is paused "stops delivering without losing the
  place" and that the core keeps calling `next` while paused, "which keeps the membership alive". It does not say how
  a client that leaves a consumer group when it has not fetched for a while must be configured, nor what a library
  should do when its poll cadence is slower than that bound.
- **Other Snodra libraries:** the Kotlin add-on pauses the assigned partitions and still polls to keep the group
  membership alive (`snodra-messaging@64dde1e:snodra-messaging-kafka/src/main/kotlin/com/damoreira/snodra/messaging/kafka/KafkaSubscription.kt:37`,
  its pause at line 139); the Rust add-on keeps calling `recv` and seeks back what it fetched while paused.
- **This library:** `snodra_kafka/kafka.py` (`MAX_POLL_INTERVAL_MS`, `KafkaSubscription.next`). The subscription keeps
  polling while paused and hands nothing out; the client's `max.poll.interval.ms` is raised to 24 h, because
  `aiokafka` measures it from the last fetch and would otherwise make a paused processor leave its group. A dead
  process is still found by the session timeout (10 s by default), which the heartbeat task misses as soon as the
  event loop is blocked.
- **Question:** open: should the specification name the bound, or is "keeps the membership alive" enough for every
  client? Recommend naming it once ("a paused subscription stays a member however long it is paused") and leaving the
  client setting to the add-on.
- **Wire change:** no.

### SG-25: an acknowledgement after a Kafka segment changed hands

- **Specification:** `event-store-add-ons.md` says an acknowledgement "acknowledges that delivery and every earlier
  one of the segment; a later subscription of the name resumes after it", and that what a revoked segment delivered
  and was not acknowledged comes again to its next holder. It does not say what an acknowledgement of a segment the
  subscription no longer holds does.
- **Other Snodra libraries:** the Rust add-on accepts it and returns without committing (`snodra-messaging-rs@4d63802:crates/snodra-messaging-kafka/src/subscription.rs:487`,
  the assignment check at line 489); the Kotlin memory store drops it after checking the segment and position
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/memory/MemoryEventStore.kt:268`).
- **This library:** `snodra_kafka/kafka.py` (`KafkaSubscription.ack`). The segment and position are parsed, and an
  acknowledgement of a partition this subscription no longer holds is dropped: the partition's new holder starts
  from the committed position, and the record it already delivered comes again there.
- **Question:** settled: the acknowledgement is dropped.
- **Wire change:** no.

### SG-26: redelivering a Kafka position at or after the subscription's next one

- **Specification:** `event-store-add-ons.md` says redeliver "hands that delivery and the segment's later ones out
  again without leaving the subscription"; it does not say what a call with a position at or after the subscription's
  next position does.
- **Other Snodra libraries:** the Rust add-on seeks back to whatever position it is given (`snodra-messaging-rs@4d63802:crates/snodra-messaging-kafka/src/subscription.rs:509`);
  the Kotlin Kafka add-on records every redelivery as a seek, a forward one included
  (`snodra-messaging@64dde1e:snodra-messaging-kafka/src/main/kotlin/com/damoreira/snodra/messaging/kafka/KafkaSubscription.kt:127`,
  applied at lines 189-194); the memory store moves the position back only for an earlier one (SG-14 in this file,
  from `snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/memory/MemoryEventStore.kt:279`).
- **This library:** `snodra_kafka/kafka.py` (`KafkaSubscription.redeliver`). Only an earlier position moves the
  subscription's next position back and seeks the partition; a position at or after it changes nothing. A forward
  seek would skip every event between the subscription's next position and the given one, which a redelivery must
  never do; this keeps the memory store's earlier-only guard (SG-14) rather than copying the Kotlin Kafka add-on's
  forward seek.
- **Question:** settled: a position at or after the next one changes nothing.
- **Wire change:** no.

### SG-27: the capabilities of a Kafka add-on in a library without an event browser

- **Specification:** `event-store-add-ons.md` says the Kafka add-on declares every capability, `browse` included, and
  that a library whose interfaces have no event browser may still declare `browse` for a store another program can
  read. It also says the browse checks are omitted from a kit without a browser, as the Kotlin kit omits them.
- **Other Snodra libraries:** the Rust add-on declares all four capabilities and has a browser
  (`snodra-messaging-rs@4d63802:crates/snodra-messaging-kafka/src/lib.rs:78`); the Kotlin event-store interface has no
  browser (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/eventstore/EventStore.kt:28`)
  and its kit runs no browse checks.
- **This library:** `snodra_kafka/kafka.py` (`_CAPABILITIES`). This library has no event browser and the add-on
  declares `replay`, `lag` and `native-dead-letter`, not `browse`; the four browse checks are omitted from its kit run.
  The Rust-based event browser reads the same `snodra.events` topic with a Rust Kafka add-on, so nothing on the wire
  is closed to it.
- **Question:** open: should a library without a browser declare `browse` anyway? Recommend no: `browse` is a promise
  to the hub that an instance can answer the five queries, and a browser-less library would have to lie or proxy them.
  The page already allows a browser in another program.
- **Wire change:** no.

### SG-28: a Kafka client whose default partition assignor is not `range`

- **Specification:** `event-store-add-ons.md` (Kafka wire rules, Group protocol) says the official add-ons "set
  neither the group protocol nor the partition assignor: each client's default", because the Kotlin and Rust clients'
  defaults are the classic protocol with `range`; "a third-party member must offer it too".
- **Other Snodra libraries:** the Kotlin and Rust add-ons rely on their clients' defaults, which are the classic
  protocol with `range`.
- **This library:** `snodra_kafka/kafka.py` (`KafkaConfig.consumer_settings`). `aiokafka` offers only the classic
  group protocol, and its default assignor is round-robin, which would not join a Kotlin or Rust instance's group; the
  add-on sets `partition_assignment_strategy=(RangePartitionAssignor,)` explicitly, which is the rule's intent.
- **Question:** settled: a client whose default is not `range` must set it, so that every official member offers the
  assignor the group decides on.
- **Wire change:** no.

### SG-29: the atomicity of a Kafka reset's apply

- **Specification:** `event-store-add-ons.md` says an admin's `apply` "moves each range's segment's committed
  position to its `from`, all or nothing"; Kafka's offset commit answers each partition separately, and the page does
  not say what a client does when one part is refused after another was accepted.
- **Other Snodra libraries:** the Rust and Go add-ons check the group is empty, build one offset commit for every
  range and send it; neither rolls a partly applied commit back.
- **This library:** `snodra_kafka/kafka.py` (`KafkaAdmin.apply`). Every range is validated first (source, segment,
  position, end offset), then the group is checked empty, then one offset commit is sent. A validation failure or an
  active member moves nothing; a group-level commit refusal moves nothing; a broker-side per-partition refusal after
  another partition was accepted is reported as :class:`~snodra.Failed` and is not rolled back, because a
  compensating commit is itself another non-atomic request and can fail too. "All or nothing" is therefore exact at
  the validation and group level and best-effort at Kafka's per-partition boundary.
- **Question:** settled: validate all ranges and send one commit; do not attempt a rollback. A future version may add
  a compensating commit if the broker's per-partition failures become observable enough to justify it.
- **Wire change:** no.

### SG-30: a message store constructed after `App.start()` and its lifecycle

- **Specification:** `message-store-add-ons.md` (The SQL layout) says the schema is applied at startup under the
  database lock, and the state page's `L2-STORE-KIT` row says every store's schema is ready before the kit runs. It
  does not say which interface a library uses to prepare it.
- **Other Snodra libraries:** the Rust `MessageStore` trait owns `init_schema` and `check_schema`
  (`snodra-messaging-rs@4d63802:src/store/mod.rs:122`), and the Rust SQL add-on applies V1 in `start()`
  (`snodra-messaging-rs@4d63802:crates/snodra-messaging-sqlx/src/lib.rs:16`); Go has `StoreLifecycle`, which the
  builder calls with the application's clock, the schema steps and `Background`
  (`snodra-messaging-go@2d0aea9:store.go:63`). Kotlin applies the SQL schema through Spring/Flyway, outside the
  store interface.
- **This library:** the Python core exports `StoreLifecycle` (`snodra/_store.py:1107`) and `App.start()` hands the
  store the application's clock, calls `init_schema()` or `check_schema()` according to
  `Settings.store_init_schema`, and starts `background()` in a `snodra:store-background` task
  (`snodra/_app.py:604`, `:620`, `:635`). `SqlStore` and `MongoStore` implement the public protocol and re-export it
  from their packages; a failing schema step fails `start()`, and a background error is logged.
- **Remaining deviation:** a store constructed after `App.start()` has already run is not prepared or backgrounded by
  the app. SQL then needs `await store.init_schema()` itself (and `store.background()` if its purge is wanted); a
  Mongo store needs `init_schema()`/`check_schema()` for its indexes. The READMEs say so.
- **Question:** settled: the core binds the lifecycle; only a store swapped in after `start()` is the service's
  responsibility. The specification should say that a late store is not prepared by the app.
- **Wire change:** no.

### SG-31: an empty outbox claim owner

- **Specification:** `message-store-add-ons.md` (The stores) says a claim returns entries "now held by the claimer
  until its lease passes", and names the owner as the claimer; it does not say what an empty owner means.
- **Other Snodra libraries:** the Go SQL and MongoDB add-ons refuse an empty owner with `ErrInvalid`
  (`snodra-messaging-go@2d0aea9:sql/outbox.go:59`, `mongo/outbox.go:46`); the Rust SQL add-on binds it as it is
  (`snodra-messaging-rs@4d63802:crates/snodra-messaging-sqlx/src/outbox.rs:151`), and the Kotlin stores do not check it
  (`snodra-messaging@64dde1e:snodra-messaging-jdbc/src/main/kotlin/com/damoreira/snodra/messaging/jdbc/JdbcOutboxStore.kt:40`).
- **This library:** `snodra_sql/sql.py` (`_SqlOutbox.claim`) refuses an empty owner with `InvalidMessage`: an empty
  owner is indistinguishable from "nobody" in the SQL rows, so a second empty-owner relay would be given the first
  one's entries. `snodra_mongo/mongo.py` (`_MongoOutbox.claim`) does the same: `{owner: null}` also matches an entry
  that no relay holds, so an empty owner would claim another empty-owner relay's entries.
- **Question:** is an empty owner invalid on every store? Recommend yes, and the specification should say so; the
  other add-ons would then refuse it too.
- **Wire change:** no.

### SG-32: a service connection bound to `transaction(conn=...)` that is not in a transaction

- **Specification:** `message-store-add-ons.md` (The stores) says an add-on joins the caller's transaction and never
  writes beside it, and `The rule` says the service's writes and the library's commit together. It does not say what
  an add-on does with a connection the service hands it that is not in a transaction, or whose transaction already
  ended.
- **Other Snodra libraries:** Go's `WithConn` documents that the store sends its write on the connection as it is and
  the connection's transaction is the service's to manage (`snodra-messaging-go@2d0aea9:sql/sql.go:491`). The Rust
  scope refuses a transaction that is not the store's (`snodra-messaging-rs@4d63802:crates/snodra-messaging-sqlx/src/db.rs:805`);
  Kotlin follows Spring's transaction manager.
- **This library:** `SqlStore.transaction(conn=conn)` binds the given `asyncpg` or `asyncmy` connection with the
  core's public `bind_transaction`, writes on it and never begins, commits or rolls back anything on it. On a
  connection in autocommit each write commits at once, and a connection whose transaction ended accepts the write in
  a new one; the store cannot tell the two apart without driver-specific transaction inspection.
- **Question:** should the add-on refuse a connection it can see is not in a transaction, or is the service
  responsible for the connection's state? Recommend the latter, as Go documents: a driver's way of reporting an open
  transaction differs, and a false refusal would break a legitimate service-managed transaction.
- **Wire change:** no.

### SG-33: MySQL applies the schema statement by statement

- **Specification:** `message-store-add-ons.md` (The SQL layout) says the schema is applied under a database lock so
  instances starting together apply it once, and that the files are idempotent; it does not say whether applying a
  file is atomic when the database commits DDL on its own.
- **Other Snodra libraries:** the Go SQL add-on runs the PostgreSQL script in one transaction and each MySQL
  statement in turn (`snodra-messaging-go@2d0aea9:sql/schema.go:199`); the Rust SQL add-on does the same
  (`snodra-messaging-rs@4d63802:crates/snodra-messaging-sqlx/src/schema.rs:218`). Kotlin applies the files through
  Flyway, which also cannot roll MySQL DDL back.
- **This library:** `snodra_sql/sql.py` (`SqlStore._apply_schema`) wraps the PostgreSQL statements in one transaction;
  on MySQL each statement commits as it runs. A failure midway leaves the statements before it applied; because every
  statement is `IF NOT EXISTS` or `INSERT ... ON DUPLICATE KEY`, a retry of `init_schema()` finishes the file. The
  schema lock is released in either case.
- **Question:** settled: applying the file is idempotent and retryable, not atomic on MySQL; the specification should
  say so, since "applied once" can otherwise be read as all-or-nothing.
- **Wire change:** no.

### SG-34: a schema check and a retention that changed

- **Specification:** `message-store-add-ons.md` says every store's schema is ready before the kit runs and that the
  schema is prepared at startup; the MongoDB layout says every instance sharing a database must be given the same
  retentions, because MongoDB refuses an index under an existing name with other options. Neither says what a schema
  check does when an index exists with a different TTL, that is, when a database was prepared with another retention.
- **This library:** `snodra_mongo/mongo.py` (`MongoStore.check_schema`). It reads every collection's indexes and
  compares each layout index's keys, TTL (whole seconds), uniqueness and partial filter; a TTL that differs from the
  store's settings is a `MessageStoreFailed` naming the index and saying that every instance sharing a database needs
  the same retentions. It creates nothing.
- **Question:** should a changed retention be a schema failure (as here) or should a check ignore TTLs? Recommend the
  former: two instances with different retentions would otherwise let either one's TTL monitor delete documents the
  other still needs.
- **Wire change:** no.

### SG-35: a retention with a fraction of a second, and a TTL index

- **Specification:** the MongoDB layout says a retention index carries the configured retention, and lists the
  defaults as whole days; MongoDB's `expireAfterSeconds` is a 32-bit integer number of seconds. Neither page says
  what a sub-second or oversized retention becomes, nor whether a store refuses one.
- **This library:** `snodra_mongo/mongo.py` (`_ttl_seconds`). A positive retention is floored to whole seconds, so
  90.9 s becomes 90 and 0.5 s becomes 0 (the TTL monitor may then expire the document at once); a zero or negative
  retention is 0; a retention beyond `2**31 - 1` seconds is refused when the config is built.
- **Question:** open: should the specification define the floor, or require whole-second retentions and refuse a
  fraction? Recommend the floor: a fraction is a configuration mistake, but refusing it would make a database shared
  with another instance whose retention is whole seconds fail for a value MongoDB never sees.
- **Wire change:** no.

### SG-36: the Python stream client's high-level producer and consumer cannot carry the wire rules

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says a subscription's consumer carries `snodra-member=<a random id per subscription instance>`, that a subscription
  answers the broker's consumer update with the stored offset (the next one to read), that reads which decide go to
  each stream's leader, and that the official add-ons "use each client's low-level client class: the high-level APIs
  can neither set `snodra-member` nor answer a consumer update with a stored offset". It names no Python client.
- **Other Snodra libraries:** Kotlin pins `com.rabbitmq:stream-client` 1.13.1
  (`snodra-messaging@64dde1e:snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitConnector.kt:25`)
  and Rust pins `rabbitmq-stream-client` 0.11.0; both use the client's low-level consumer. The Go client exports no
  low-level class, so the Go add-on speaks the protocol's commands itself.
- **This library:** `snodra_rabbitmq/rabbitmq.py` pins `rstream==1.1.1` and uses its low-level
  `rstream.client.Client` with the `rstream.schema` frames and `rstream.amqp.amqp_decoder`; it does not use
  `rstream.SuperStreamProducer` or `rstream.SuperStreamConsumer`. The spike (`CLIENT-SPIKE.md`) found that the
  high-level producer routes a key with UTF-16 bytes and a 128-bit `mmh3.hash_bytes` read little-endian
  (`rstream/superstream.py:88`), not the wire rule's x86_32 of the UTF-8 bytes, and that the high-level consumer
  connects to a random replica (`rstream/consumer.py:201`), not the leader. The consumer update reply is **not** a
  blocker: `rstream.OffsetSpecification` is public (`rstream/__init__.py:44`) and encodes it;
  `rstream.ConsumerOffsetSpecification` is the different type `Consumer.subscribe` takes for its initial offset, not
  a listener return. The low-level client sets all four subscribe properties, answers updates with a stored offset,
  publishes one confirmed message per event and reads offsets on the leader.
  `test_a_second_instance_shares_the_processor_and_the_member_is_visible` and
  `test_the_stored_offset_is_the_next_one_and_a_new_subscription_resumes` pin those on a real broker.
- **Question:** should the specification name the low-level capabilities a Python client must expose (so another
  client can replace `rstream`), or require the client's high-level API to add leader selection and the wire rules'
  routing? The wire rules' statement that a high-level API can neither set `snodra-member` nor answer a consumer
  update with a stored offset does not hold for `rstream`.

### SG-37: a management-API consumer without `snodra-member`

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says a subscription's members are "the distinct `snodra-member` values of the consumers of that name on the
  source's partitions". A consumer written with another client reads the same stream without the property.
- **Other Snodra libraries:** Kotlin falls back to the consumer's connection name and subscription id
  (`snodra-messaging@64dde1e:snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitManagement.kt:185`),
  Rust does the same (`snodra-messaging-rs@4d63802:crates/snodra-messaging-rabbitmq/src/management.rs:197`), and the
  Go add-on does too.
- **This library:** `snodra_rabbitmq/rabbitmq.py` (`parse_consumers`, `RabbitMQAdmin.members`) falls back to
  `<connection name>:<subscription id>` and counts the consumer as a member, so a reset refuses while it reads. A
  consumer without `properties.name` is an unnamed reader (a browser's) and is not a member.
- **Question:** is a consumer without the property a member (a reset must refuse while it reads), or is it invisible
  to the members list?

### SG-38: reading a stream's first offset and head

- **Specification:** [event-store-add-ons.md, Segments and positions](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#segments-and-positions)
  makes positions opaque and the RabbitMQ wire rules say a position is the offset, but no page says how an add-on
  learns a stream's first offset and head. The stream protocol's statistics name chunk ids, not offsets, and
  offset-tracking entries take offsets of their own.
- **Other Snodra libraries:** Kotlin reads the first and last chunk of events on the leader, with a quiet period
  (`snodra-messaging@64dde1e:snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitReads.kt:100`,
  `:175`); Rust does the same (`snodra-messaging-rs@4d63802:crates/snodra-messaging-rabbitmq/src/read.rs:134`), and the
  Go add-on follows them.
- **This library:** `snodra_rabbitmq/rabbitmq.py` (`RabbitMQEventStore._bounds`, `_read_chunk`) subscribes once with
  `first` and once with `last` on the partition's leader; the first chunk's first offset is the first position, and
  the last chunk's first offset plus its record count is the head. A stream is empty only when both reads are silent
  within 1.5 s; one silent read beside one that answered is a failure, never a shortened or empty stream. The
  subscription's head refresher and the admin's plan use it.
- **Question:** should the specification define how a stream's bounds are read (or a minimum bound), or leave it to
  each add-on?

### SG-39: `browse` on a library without an event browser

- **Specification:** [RabbitMQ Streams wire rules](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules)
  says the RabbitMQ Streams add-on declares `browse`, `lag` and `native-dead-letter`, and `replay` with its
  management API; the same page says a library without an event browser may still declare `browse` for a store another
  program can read. The Python core has no event-browser interface.
- **Other Snodra libraries:** Rust has a browser and declares `browse`; Kotlin has no browser interface and the Go
  core has none either, so the Go add-on declares no `browse`.
- **This library:** `snodra_rabbitmq/rabbitmq.py` (`RabbitMQEventStore.capabilities`) declares `lag`,
  `native-dead-letter` and (with the management API) `replay`, never `browse`; there is no `browser()` to return. A
  browser program with its own add-on can still read the streams.
- **Question:** should the Python and Go add-on pages say `browse` is not declared when the library has no browser, or
  should the add-on declare it for a separate browser program?

### SG-40: when the management API's metrics are off

- **Specification:** [event-store-add-ons.md, Capabilities](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#capabilities)
  says the RabbitMQ add-on declares `replay` only when its management API is configured, because only that API lists
  the instances reading a subscription. No page says what a store does when the API is configured but its metrics
  collection is off (`management.disable_stats` or `management_agent.disable_metrics_collector`): its consumer list
  then answers nothing.
- **Other Snodra libraries:** Kotlin's `RabbitEventStore.open` asks the overview once and builds the store without
  `replay` when the metrics are off (`snodra-messaging@64dde1e:snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitEventStore.kt:144`);
  Rust's `RabbitEventStore::open` does the same
  (`snodra-messaging-rs@4d63802:crates/snodra-messaging-rabbitmq/src/lib.rs:228`), and the Go add-on's `Open`
  follows them.
- **This library:** the core's event-store port has no `open` step, so `RabbitMQEventStore` declares `replay` from
  the configured management URL when it is constructed; `RabbitMQAdmin.members` asks `/api/overview` and fails with
  `Failed` when `disable_stats` is true, so every reset asks the consumer list and never reads "nobody reads" from
  untrustworthy statistics. A management API that is down (or has its metrics off) fails `members` and `apply`;
  `plan` reads only the streams and does not ask it. The constructor does not fail.
- **Question:** should disabled metrics be a startup failure or a store without `replay`, and should the event-store
  port have an explicit open/verify step rather than the constructor's configuration?

### SG-41: a reset is all or nothing over independently stored offsets

- **Specification:** [event-store-add-ons.md, The admin](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-admin)
  says `apply(ranges)` "moves each range's segment's committed position to its `from`, all or nothing". RabbitMQ
  Streams has no command that stores several partitions' offsets together: each is its own asynchronous command on its
  partition's leader, and a failure after some were stored leaves them moved.
- **Other Snodra libraries:** Kotlin stores each range in turn
  (`snodra-messaging@64dde1e:snodra-messaging-rabbitmq/src/main/kotlin/com/damoreira/snodra/messaging/rabbitmq/RabbitSubscriptionAdmin.kt:54`),
  Rust does the same (`snodra-messaging-rs@4d63802:crates/snodra-messaging-rabbitmq/src/admin.rs:235`), and the Go
  add-on follows them.
- **This library:** `snodra_rabbitmq/rabbitmq.py` (`RabbitMQAdmin.apply`) validates every range and every head first,
  refuses while a member reads, stores each `from` in turn, then reads every one back. A store or read-back failure is
  reported; the offsets already stored are not restored. A range read back at another offset is
  `SubscribersActive`.
- **Question:** should a reset roll back (best effort) when a later partition's store fails, or should the
  specification say a failed apply may leave some partitions moved and the planner re-runs it?

### SG-42: a subscription to a source that does not exist

- **Specification:** [event-store-add-ons.md, The operations](https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-operations)
  says subscribe joins the subscription's name on its source as one more instance, and that a new subscription's first
  event is an assignment, "an empty one when it holds no segment, within a bound of its joining (10 s in both
  libraries, `FIRST_ASSIGNMENT_WITHIN`)". The RabbitMQ wire rules also say the library creates no stream ("the
  infrastructure does"). No page says what subscribe does while the source does not exist.
- **Other Snodra libraries:** Kafka's Go add-on waits for the topic to appear and the consumer group joins then; the
  Go RabbitMQ add-on reads the source's layout and fails when neither a super stream nor a plain stream exists, so the
  core resubscribes after its backoff. The Kotlin and Rust RabbitMQ add-ons read the layout when joining too.
- **This library:** `snodra_rabbitmq/rabbitmq.py` (`RabbitMQEventStore.subscribe` and `_layout`) raises `Failed` when
  neither a super stream nor a plain stream named the source exists. The core closes the subscription and retries
  after its backoff, so a stream created later is picked up; a source that never appears never yields an empty first
  assignment. A publish to a missing source is `Failed` too, never `Rejected`: the relay retries it.
- **Question:** should subscribe wait for the source (making the 10 s first-assignment promise unprovable for a source
  that never appears), or refuse it as this library does?

### SG-43: an explicit `processor="default"`

- **Specification:** [protocol.md, Processor status](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-status)
  says the name is "`default` for the handlers without a processing group, else the group name
  (`[a-z0-9][a-z0-9-]{0,47}`)"; Kotlin and Rust read that as "a named processor's name is not `default`". Python's
  `App.event_handler(processor=...)` has `"default"` as the parameter's own default, so the call cannot tell an omitted
  parameter from an explicit `"default"`.
- **Other Snodra libraries:** Kotlin refuses `@ProcessingGroup("default")`
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/handler/ProcessingGroup.kt:28`,
  the error built at `.../handler/HandlerRegistry.kt:110`); Rust's `event_in` refuses it
  (`snodra-messaging-rs@4d63802:src/processors/mod.rs:83`). Both have a separate `event()`/bare annotation for the
  unnamed processor.
- **This library:** `App.event_handler(processor="default")` registers in `default`, exactly as omitting the parameter
  does; `tests/test_processors.py::test_the_processor_name_is_checked_and_default_is_the_unnamed_one` pins it. Every
  other name is checked against the pattern and the 48-character bound.
- **Question:** should Python take a sentinel (or a separate `default_processor=True`) so that an explicit `default` is
  refused as the other libraries refuse it, or is the parameter's default the API's statement that `default` is the
  unnamed processor's valid spelling?
- **Wire change:** no.


### SG-44: a message store without the `processor_control` port

- **Specification:** [protocol.md, Processor control](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-control)
  says the control state "lives in the service's message store, which every instance reads at start and every 5 s";
  [message-store-add-ons.md](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/) freezes the
  ports and says a port not yet at its level reports `Unsupported`. No page says what an event processor does when its
  message store's control port is the unsupported one.
- **Other Snodra libraries:** Kotlin's control reads the documents before any consumer starts and, when the read fails,
  starts nothing until a poll reads them
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/control/ProcessorControl.kt:124`,
  the consumers started through `.../events/EventProcessors.kt:49`). Its message stores always implement the port, so
  it has no `Unsupported` case; the same holds for Rust's store ports.
- **This library:** `Processors._read_and_apply` treats `Unsupported` from `MessageStore.control.get` as "this store
  cannot keep control documents": that processor starts unpaused (a level-2 store keeps consuming), is remembered and
  is not polled again, with one INFO log. A pause requested for it fails at the store write (the control action
  answers `FAILED`); a document cannot be honoured. An error other than `Unsupported` still leaves the processors as
  they are and is retried at the next poll. Pinned by
  `tests/test_processors.py::test_a_store_without_processor_control_starts_the_processors_unpaused`.
- **Question:** should a store without the port be a startup error at level 3 (as a level claim requires every port),
  or is the silent unpaused fallback the right one for a library used against a level-2 add-on?
- **Wire change:** no.

### SG-45: a message store without the dead-letter port in mode `collection`

- **Specification:** [protocol.md, Dead letters](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-control)
  says a record that exhausts its attempts is "parked in `collection` (default) at
  `<processor>:<source>:<segment>:<position>`"; [message-store-add-ons.md](https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/)
  freezes the dead-letter port and says a port not yet at its level reports `Unsupported`. No page says what a
  processor does when the store cannot park.
- **Other Snodra libraries:** Kotlin parks through its `DeadLetterStore`; a sink failure leaves the record
  unacknowledged and hands it out again after the backoff
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:489`,
  the delivery path at `.../EventConsumer.kt:559`). Its stores implement the port, so it has no `Unsupported` case.
- **This library:** `EventConsumer._park_work` catches `Unsupported` and treats it as "this store cannot keep dead
  letters": it logs the store at error, records it as the processor's `last_error`, and acknowledges the record as
  lost instead of redelivering it forever. A `head`/`get` that is `Unsupported` during sequencing means the same:
  the record is handled without sequencing it. Any other store error still redelivers and retries in place (no
  rebalance storm). Pinned by
  `tests/test_deadletters.py::test_a_store_without_dead_letters_acknowledges_the_failed_record_as_lost` and
  `::test_a_failing_parked_key_check_redelivers_in_place_without_resubscribing`.
- **Question:** is "acknowledge as lost" acceptable for a level-2 store used in mode `collection`, or should the
  library refuse the mode at startup when the port is unsupported (and how would it tell without a probe)?
- **Wire change:** no.

### SG-46: how long a reset prepare waits for the record in hand

- **Specification:** [protocol.md, Processor control](https://snodra.damoreira.com/docs/library-authors/protocol/#processor-control)
  says a `reset_prepare` leaves the subscription and that the hub gives up after 10 s; it does not say how long a record
  already in hand may finish before the instance answers.
- **Other Snodra libraries:** Kotlin's prepare calls `EventConsumer.stop`, which joins the reader thread for up to 30 s
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/events/EventConsumer.kt:107`,
  joined at `.../EventConsumer.kt:250`): a handler still running after that is left behind and a second reader is not
  started beside it.
- **This library:** `Processors.set_running(..., run=False)` waits at most `Timings.handler_drain` (5 s shipped) for a
  record in hand, acknowledges it and leaves the subscription; a handler that outlives that is cancelled, its
  transaction rolled back and the record redelivered to the next subscription. The shorter bound keeps the prepare's
  answer within the hub's 10 s control timeout. Pinned by
  `tests/test_reset.py::test_a_prepare_that_cannot_finish_the_record_cancels_it_and_the_record_comes_again`.
- **Question:** should prepare finish a record in hand within the hub's bound (and answer `ok` only then), as this
  library does, or leave it to the next subscription at once (a shorter wait, fewer odds of a late answer)?
- **Wire change:** no.

### SG-47: a custom idempotency key for an external-call adapter

- **Specification:** [messages.md, Request headers](https://snodra.damoreira.com/docs/library-authors/messages/#request-headers)
  says an idempotent adapter uses the `idempotency-key` header to act once and does not say whether the key may be
  derived from the message. Kotlin's public `@IdempotentHandler` takes a SpEL key over the decoded message and the
  headers and compiles it at registration
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/external/IdempotentHandler.kt:23-25,34,48`);
  its default is the same header, and an adapter that polls can put the attempt in the key.
- **Other Snodra libraries:** Go's `HandleIdempotentCommand` reads only the header and records the decision as SG-53.
- **This library:** `snodra/_external.py` reads only the `idempotency-key` header; a missing or blank one fails the call
  before the record and the body. An adapter whose key is derived from the message must set the header at the caller.
- **Question:** should Python add a typed key expression/option (recognising the message and headers), or keep the
  header-only contract? Settled for this level as the header only, matching Go.
- **Wire change:** no.

### SG-48: a panic in an external-call adapter body

- **Specification:** [writing-a-library.md, The failure notice](https://snodra.damoreira.com/docs/library-authors/writing-a-library/#the-failure-notice)
  says `UNKNOWN_OUTCOME` is for an idempotent adapter that cannot tell whether its external call happened; no page
  says what a panic in the adapter body does. Kotlin catches `Throwable` and settles the record `FAILED`
  (`snodra-messaging@64dde1e:snodra-messaging/src/main/kotlin/com/damoreira/snodra/messaging/external/IdempotencyInterceptor.kt:84-90,193-209`),
  so its first caller sees a plain permanent failure.
- **Other Snodra libraries:** Go recovers a panic, settles the record `UNKNOWN` and returns the unknown-outcome marker
  (its SG-54).
- **This library:** `snodra/_external.py` treats a `BaseException` that is not an `Exception` (and is not
  `asyncio.CancelledError`) as a panic: it settles the record `UNKNOWN`, then raises
  `snodra.UnknownOutcome`, so the first caller and a redelivery agree and the body is not run again automatically.
- **Question:** should a panic be `UNKNOWN` (this library and Go, the safe answer) or `FAILED` (Kotlin)? Settled as
  `UNKNOWN` for this library.
- **Wire change:** no.
