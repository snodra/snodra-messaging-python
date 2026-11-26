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
  `current_message().correlation_id` is `""`, and the requests it sends carry `correlationId: ""` on.
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
