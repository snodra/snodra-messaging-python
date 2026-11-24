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
  message, and the wire message for every other code. A handler's `Unauthenticated`, `PermissionDenied` and
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
